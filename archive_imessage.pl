#!/usr/bin/perl -w

use warnings;
use strict;

use Digest::SHA1 qw(sha1 sha1_hex);
use DBI;
use Encode qw( decode_utf8 );
use POSIX;
use File::Copy;

# !!! FIXME: this isn't installed by default on Mac OS X and we can't probably do without.
use Date::Manip qw(UnixDate);

my $VERSION = '0.0.1';

my $gaptime = (30 * 60);
my $timezone = strftime('%Z', localtime());
my $now = time();
my $tmpemail = undef;

# Fixes unicode dumping to stdio...hopefully you have a utf-8 terminal by now.
#binmode(STDOUT, ":utf8");
#binmode(STDERR, ":utf8");

sub signal_catcher {
    my $sig = shift;
    fail("Caught signal ${sig}!");
}
$SIG{INT} = \&signal_catcher;
$SIG{TERM} = \&signal_catcher;
$SIG{HUP} = \&signal_catcher;

my $redo = 0;

my $debug = 0;
sub dbgprint {
    print @_ if $debug;
}

my $archivedir = undef;
my $imessageuser = undef;
my $maildir = undef;
my $writing = 0;

sub fail {
    my $err = shift;
    close(TMPEMAIL) if ($writing);
    $writing = 0;
    unlink($tmpemail) if (defined $tmpemail);
    die("$err\n");
}


my $startid = 0;
my %startids = ();
my $lastarchivefname = undef;
my $lastarchivetmpfname = undef;

sub flush_startid {
    my ($with, $msgid) = @_;
    $startids{$with} = $msgid;
    my $startval = 0;
    $startval = $startid if (defined $startid);
    dbgprint("flushing startids (global: $startval, '$with': $msgid)\n");
    if (not open(LASTID,'>',$lastarchivetmpfname)) {
        dbgprint("Open '$lastarchivetmpfname' for write failed: $!\n");
        return 0;
    }
    print LASTID "$startval\n";
    foreach(keys %startids) {
        my $key = $_;
        my $val = $startids{$key};
        print LASTID "$key\n$val\n";
    }
    close(LASTID);

    if (not move($lastarchivetmpfname, $lastarchivefname)) {
        unlink($lastarchivetmpfname);
        dbgprint("Rename '$lastarchivetmpfname' to '$lastarchivefname' failed: $!\n");
        return 0;
    }
    return 1;
}

my $outmsgid = undef;
my $outhandle_id = undef;
my $outid = undef;
my $outtimestamp = undef;
sub flush_conversation {
    my $trash = shift;
    return if (not defined $tmpemail);
    if ($writing) {
        close(TMPEMAIL);
        $writing = 0;
        if ($trash) {
            dbgprint("Trashed conversation in '$tmpemail'\n");
            unlink($tmpemail);
            return;
        }

        fail("message id went backwards?!") if ($startids{$outhandle_id} > $outmsgid);

        my $size = (stat($tmpemail))[7];
        my $t = $outtimestamp;
        my $outfile = "$t.$outid.imessage-chatlog.$imessageuser,S=$size";
        $outfile =~ s#/#_#g;
        $outfile = "$maildir/new/$outfile";
        if (move($tmpemail, $outfile)) {
            utime $t, $t, $outfile;  # force it to collection creation time.
            dbgprint "archived '$outfile'\n";
            system("cat $outfile") if ($debug);
        } else {
            unlink($outfile);
            fail("Rename '$tmpemail' to '$outfile' failed: $!");
        }

        # !!! FIXME: this may cause duplicates if there's a power failure RIGHT HERE.
        if (not flush_startid($outhandle_id, $outmsgid)) {
            unlink($outfile);
            fail("didn't flush startids");
        }
    }
}

sub split_date_time {
    my $timestamp = shift;
    my $date = UnixDate("epoch $timestamp", '%Y-%m-%d');
    my $time = UnixDate("epoch $timestamp", '%H:%M');
    dbgprint("split $timestamp => '$date', '$time'\n");
    return ($date, $time);
}

sub usage {
    print STDERR "USAGE: $0 [--debug] [--redo] <backupdir> <maildir>\n";
    print STDERR "\n";
    print STDERR "    --debug: enable spammy debug logging to stdout.\n";
    print STDERR "    --redo: Rebuild from scratch instead of only new bits.\n";
    print STDERR "    backupdir: Directory holding unencrypted iPhone backup.\n";
    print STDERR "    maildir: Path of Maildir where we write archives and metadata.\n";
    print STDERR "\n";
    exit(1);
}

foreach (@ARGV) {
    $debug = 1, next if $_ eq '--debug';
    $debug = 0, next if $_ eq '--no-debug';
    $redo = 1, next if $_ eq '--redo';
    $redo = 0, next if $_ eq '--no-redo';
    $archivedir = $_, next if not defined $archivedir;
    $maildir = $_, next if (not defined $maildir);
    usage();
}
usage() if not defined $archivedir;
usage() if not defined $maildir;

# don't care if these fail.
mkdir("$maildir", 0700);
mkdir("$maildir/tmp", 0700);
mkdir("$maildir/cur", 0700);
mkdir("$maildir/new", 0700);

$tmpemail = "$maildir/tmp/imessage-chatlog-tmp-$$.txt";

$lastarchivetmpfname = "$maildir/tmp_imessage_last_archive_msgids.txt";
unlink($lastarchivetmpfname);

$lastarchivefname = "$maildir/imessage_last_archive_msgids.txt";
unlink($lastarchivefname) if ($redo);
if (open(LASTID,'<',$lastarchivefname)) {
    my $globalid = <LASTID>;
    chomp($globalid);
    $startid = $globalid if ($globalid =~ /\A\d+\Z/);
    dbgprint("startid (global) == $globalid\n");
    while (not eof(LASTID)) {
        my $user = <LASTID>;
        chomp($user);
        my $id = <LASTID>;
        chomp($id);
        if ($id =~ /\A\d+\Z/) {
            $startids{$user} = $id;
            dbgprint("startid '$user' == $id\n");
        }
    }
    close(LASTID);
}

sub archive_fname {
    my $domain = shift;
    my $name = shift;
    return "$archivedir/" . sha1_hex("$domain-$name");
}

my $dbname = archive_fname('HomeDomain', 'Library/SMS/sms.db');
dbgprint("message database is '$dbname'\n");
my $db = DBI->connect("DBI:SQLite:dbname=$dbname", '', '', { RaiseError => 0 })
    or fail("Couldn't open message database at '$archivedir/$dbname': " . $DBI::errstr);

my $addressbookdbname = archive_fname('HomeDomain', 'Library/AddressBook/AddressBook.sqlitedb');
dbgprint("address book database is '$addressbookdbname'\n");
my $addressbookdb = DBI->connect("DBI:SQLite:dbname=$addressbookdbname", '', '', { RaiseError => 0 })
    or fail("Couldn't open addressbook database at '$archivedir/$addressbookdbname': " . $DBI::errstr);

my $stmt;

my %longnames = ();
my %shortnames = ();


dbgprint("Sorting out real names...\n");

sub parse_addressbook_name {
    my @lookuprow = @_;
    my $address = shift @lookuprow;
    my $nickname = shift @lookuprow;
    my $firstname = $lookuprow[0];
    my $proper = undef;

    dbgprint("ADDRESSBOOK NICKNAME: $nickname\n") if defined $nickname;

    foreach (@lookuprow) {
        next if not defined $_;
        dbgprint("ADDRESSBOOK ROW: $_\n");
        $proper .= ' ' if defined $proper;
        $proper .= $_;
    }

    if (defined $nickname) {
        if (not defined $proper) {
            $proper = $nickname;
        } else {
            $proper .= " (\"$nickname\")";
        }
    }

    if (not defined $proper) {
        dbgprint("WARNING: No proper name in the address book for '$address'\n");
        $proper = $address;
    }

    my $longname = $proper;
    my $shortname;
    if (defined $nickname) {
        $shortname = $nickname;
    } elsif (defined $firstname) {
        $shortname = $firstname;
    } else {
        $shortname = $proper;
    }

    return ($longname, $shortname);
}

my $lookupstmt = $addressbookdb->prepare('select c15Phone, c16Email, c11Nickname, c0First, c2Middle, c1Last from ABPersonFullTextSearch_content where ((c15Phone LIKE ?) or (c16Email LIKE ?)) limit 1;')
    or fail("Couldn't prepare name lookup SELECT statement: " . $DBI::errstr);

$stmt = $db->prepare('select m.handle_id, h.id from handle as h inner join (select distinct handle_id from message) m on m.handle_id=h.ROWID;')
    or fail("Couldn't prepare distinct address SELECT statement: " . $DBI::errstr);
$stmt->execute() or fail("Couldn't execute distinct address SELECT statement: " . $DBI::errstr);

while (my @row = $stmt->fetchrow_array()) {
    my ($handleid, $address) = @row;
    my $proper = undef;

    my $like = "%$address%";
    dbgprint("looking for $address...\n");

    $lookupstmt->execute($like, $like) or fail("Couldn't execute name lookup SELECT statement: " . $DBI::errstr);

    my @lookuprow = $lookupstmt->fetchrow_array();
    if (@lookuprow) {
        my $phone = shift @lookuprow;
        my $email = shift @lookuprow;
        dbgprint("EMAIL: $email\n") if defined $email;
        dbgprint("PHONE: $phone\n") if defined $phone;
    } else {
        @lookuprow = (undef, undef, undef, undef);
    }

    ($longnames{$handleid}, $shortnames{$handleid}) = parse_addressbook_name($address, @lookuprow);
    dbgprint("longname for $address ($handleid) == " . $longnames{$handleid} . "\n");
    dbgprint("shortname for $address ($handleid) == " . $shortnames{$handleid} . "\n");
}

# We use -1 for iPhone owner in the chat.
fail('Already a handle_id of -1?!') if defined $longnames{-1};
fail('Already a handle_id of -1?!') if defined $shortnames{-1};

# !!! FIXME: is the owner always the first entry in the address book?
$lookupstmt = $addressbookdb->prepare('select ROWID, Nickname, First, Middle, Last from ABPerson order by ROWID limit 1;')
    or fail("Couldn't prepare owner lookup SELECT statement: " . $DBI::errstr);
if (not $lookupstmt->execute()) {
    fail("Couldn't execute owner lookup SELECT statement: " . $DBI::errstr);
} else {
    my @lookuprow = $lookupstmt->fetchrow_array();
    fail("Couldn't find iPhone owner's address book entry?!") if not @lookuprow;
    my $rowid = shift @lookuprow;
    ($longnames{-1}, $shortnames{-1}) = parse_addressbook_name('me', @lookuprow);
    # Ok, let's get the email address or phone number for this user.
    $lookupstmt = $addressbookdb->prepare('select value from ABMultiValue where record_id=? and property=? order by UID limit 1;')
        or fail("Couldn't prepare owner phone/email lookup SELECT statement: " . $DBI::errstr);

    # 4 is email address, 3 is phone number.
    $lookupstmt->execute($rowid, 4) or fail("Couldn't execute owner phone/email lookup SELECT statement: " . $DBI::errstr);
    @lookuprow = $lookupstmt->fetchrow_array();
    if (@lookuprow) {
        $imessageuser = shift @lookuprow;
    } else {
        $lookupstmt->execute($rowid, 3) or fail("Couldn't execute owner phone/email lookup SELECT statement: " . $DBI::errstr);
        @lookuprow = $lookupstmt->fetchrow_array();
        fail("Couldn't find email or phone number of iPhone owner.") if not @lookuprow;
        $imessageuser = shift @lookuprow;
        $imessageuser =~ s/[^\d]//g;
        $imessageuser = "phone-$imessageuser\@icloud.com";
    }
}

dbgprint("longname for imessageuser ($imessageuser) == " . $longnames{-1} . "\n");
dbgprint("shortname for imessageuser ($imessageuser) == " . $shortnames{-1} . "\n");

$lookupstmt = undef;
$addressbookdb->disconnect();
$addressbookdb = undef;


dbgprint("Parsing messages...\n");

sub talk_gap {
    my ($a, $b) = @_;
    my $time1 =  ($a < $b) ? $a : $b;
    my $time2 =  ($a < $b) ? $b : $a;
    return (($time2 - $time1) >= $gaptime);
}

my $lastspeaker = '';
my $lastdate = 0;
my $lastday = '';
my $lasthandle_id = 0;
my $alias = undef;
my $thisimessageuseralias = $shortnames{-1};
my $startmsgid = undef;
my $newestmsgid = 0;
$startid = undef;

$stmt = $db->prepare('select h.id, m.ROWID, m.text, m.service, m.account, m.handle_id, m.subject, m.date, m.is_from_me, m.was_downgraded, m.is_audio_message, m.cache_has_attachments from message as m inner join handle as h on m.handle_id=h.ROWID order by m.ROWID;')
    or fail("Couldn't prepare message SELECT statement: " . $DBI::errstr);

my $attachmentstmt = $db->prepare('select filename, mime_type from attachment as a inner join (select rowid,attachment_id from message_attachment_join where message_id=?) as j where a.ROWID=j.attachment_id order by j.ROWID;')
    or fail("Couldn't prepare attachment lookup SELECT statement: " . $DBI::errstr);

$stmt->execute() or fail("Couldn't execute message SELECT statement: " . $DBI::errstr);
while (my @row = $stmt->fetchrow_array()) {
    if ($debug) {
        dbgprint("New row:\n");
        foreach(@row) {
            dbgprint(defined $_ ? "  $_\n" : "  [undef]\n");
        }
    }

    my ($idname, $msgid, $text, $service, $account, $handle_id, $subject, $date, $is_from_me, $was_downgraded, $is_audio_message, $cache_has_attachments) = @row;
    next if not defined $text;

    # Convert from Cocoa epoch to Unix epoch (2001 -> 1970).
    $date += 978307200;

    # !!! FIXME: do something if defined $subject
    # !!! FIXME: do something with $service

    if ($cache_has_attachments) {
        $attachmentstmt->execute($msgid) or fail("Couldn't execute attachment lookup SELECT statement: " . $DBI::errstr);
        while (my @attachmentrow = $attachmentstmt->fetchrow_array()) {
        }
    }

    $startmsgid = $msgid if (not defined $startmsgid);

    if (not defined $startids{$handle_id}) {
        my $with = $longnames{$handle_id};
        dbgprint("Flushing new per-user startid for $idname ('$with')\n");
        if (not flush_startid($handle_id, 0)) {
            fail("didn't flush new startid for $idname ('$with')");
        }
    }

    if (($now - $date) < $gaptime) {
        dbgprint("timestamp '$date' is less than $gaptime seconds old.\n");
        if ((not defined $startid) or ($msgid < $startid)) {
            dbgprint("forcing global startid to $startid\n");
            $startid = ($startmsgid-1);
        }
        # trash this conversation, it might still be ongoing.
        flush_conversation(1) if ($handle_id == $lasthandle_id);
        next;
    }

    $newestmsgid = $msgid if ($msgid > $newestmsgid);

    # this happens if we had a conversation that was still ongoing when a 
    #  newer conversation got archived. Next run, the newer conversation
    #  gets pulled from the database again so we can recheck the older
    #  conversation.
    if ($msgid <= $startids{$handle_id}) {
        dbgprint("msgid $msgid is already archived.\n");
        next;
    }

    # Try to merge collections that appear to be the same conversation...
    if (($handle_id != $lasthandle_id) or (talk_gap($lastdate, $date))) {
        flush_conversation(0);

        open(TMPEMAIL,'>',$tmpemail) or fail("Failed to open '$tmpemail': $!");
        #binmode(TMPEMAIL, ":utf8");
        $outtimestamp = $date;
        $outhandle_id = $handle_id;
        $outid = $msgid;
        $writing = 1;

        $startmsgid = $msgid;

        my $fullalias = $longnames{$handle_id};
        my $shortalias = $shortnames{$handle_id};

        if ($shortalias ne $shortnames{-1}) {
            $alias = $shortalias;
            $thisimessageuseralias = $shortnames{-1};
        } elsif ($fullalias ne $longnames{-1}) {
            $alias = $fullalias;
            $thisimessageuseralias = $longnames{-1};
        } else {
            $alias = "$shortalias ($idname)";
            $thisimessageuseralias = $shortnames{-1} . " ($imessageuser)";
        }

        my $person = $idname;
        if ($person ne $fullalias) {
            $person = "$fullalias [$idname]";
        }

        my $emaildate = UnixDate("epoch $date", '%a, %d %b %Y %H:%M %Z');
        my $localdate = UnixDate("epoch $date", '%Y-%m-%d %H:%M:%S %Z');

        print TMPEMAIL "Return-Path: <$imessageuser>\n";
        print TMPEMAIL "Delivered-To: $imessageuser\n";
        print TMPEMAIL "MIME-Version: 1.0\n";
        print TMPEMAIL "Content-Type: text/plain; charset=\"utf-8\"\n";
        print TMPEMAIL "Content-Transfer-Encoding: binary\n";
        print TMPEMAIL "X-Mailer: archive_imessage.pl $VERSION\n";
        print TMPEMAIL "From: " . $longnames{-1} . " <$imessageuser>\n";
        print TMPEMAIL "To: " . $longnames{-1} . " <$imessageuser>\n";
        print TMPEMAIL "Date: $emaildate\n";
        print TMPEMAIL "Subject: Chat with $person at $localdate ...\n";

        $lasthandle_id = $handle_id;
        $lastspeaker = '';
        $lastdate = 0;
        $lastday = '';
    }

    # replace "/me does something" with "*does something*" ...
    $text =~ s#\A/me (.*)\Z#*$1*#m;

    my $speaker = $is_from_me ? $thisimessageuseralias : $alias;
    #$speaker .= " [$service]" if ($service ne 'iMessage');

    my ($d, $t) = split_date_time($date);

    if ((defined $lastday) and ($lastday ne $d)) {
        print TMPEMAIL "\n$d\n";
        $lastspeaker = '';  # force it to redraw.
    }

    print TMPEMAIL "\n$speaker:\n" if ($lastspeaker ne $speaker);
    print TMPEMAIL "$t  $text\n";

    $lastdate = $date;
    $lastday = $d;
    $lastspeaker = $speaker;
    $outmsgid = $msgid;
}

$db->disconnect();
exit(0);

# end of archive_imessage.pl ...

