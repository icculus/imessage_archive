#!/usr/bin/perl -w

use warnings;
use strict;

use Digest::SHA1 qw(sha1_hex);
use DBI;
use Encode qw( decode_utf8 );
use POSIX;
use File::Copy;
use MIME::Base64;
use File::Slurp;

my $VERSION = '0.0.1';

my $gaptime = (30 * 60);
my $timezone = strftime('%Z', localtime());
my $now = time();

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
my $imessageuserhost = undef;
my $maildir = undef;
my $allow_html = 0;

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
    $allow_html = 1, next if $_ eq '--html';
    $allow_html = 0, next if $_ eq '--no-html';
    $archivedir = $_, next if not defined $archivedir;
    $maildir = $_, next if (not defined $maildir);
    usage();
}
usage() if not defined $archivedir;
usage() if not defined $maildir;


sub fail {
    my $err = shift;
    die("$err\n");
}

sub archive_fname {
    my $domain = shift;
    my $name = shift;
    my $combined = "$domain-$name";
    my $hashed = sha1_hex($combined);
    dbgprint("Hashed archived filename '$combined' to '$hashed'\n");
    return "$archivedir/$hashed";
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

sub slurp_archived_file {
    my ($domain, $fname, $fdataref) = @_;
    my $hashedfname = archive_fname($domain, $fname);
    if ((not defined read_file($hashedfname, buf_ref => $fdataref, binmode => ':raw')) or (not defined $fdataref)) {
        fail("Couldn't read attachment '$hashedfname': $!");
    }
}


my %longnames = ();
my %shortnames = ();

my $outperson = undef;
my $outmsgid = undef;
my $outhandle_id = undef;
my $outid = undef;
my $outtimestamp = undef;
my $output_text = undef;
my $output_html = undef;
my @output_attachments = ();

sub flush_conversation {
    return if (not defined $outmsgid);

    my $trash = shift;

    dbgprint("Flushing conversation! trash=$trash\n");

    if ($trash) {
        $output_text = undef;
        $output_html = undef;
        @output_attachments = ();
        return;
    }

    fail("message id went backwards?!") if ($startids{$outhandle_id} > $outmsgid);

    $output_text =~ s/\A\n+//;
    $output_text =~ s/\n+\Z//;

    my $tmpemail = "$maildir/tmp/imessage-chatlog-tmp-$$.txt";
    open(TMPEMAIL,'>',$tmpemail) or fail("Failed to open '$tmpemail': $!");
    #binmode(TMPEMAIL, ":utf8");

    my $emaildate = strftime('%a, %d %b %Y %H:%M %Z', localtime($outtimestamp));
    my $localdate = strftime('%Y-%m-%d %H:%M:%S %Z', localtime($outtimestamp));

    my $imessageuserlongname = $longnames{-1};

    # !!! FIXME: make sure these don't collide.
    my $mimesha1 = sha1_hex($output_text);
    my $mimeboundarymixed = "mime_imessage_mixed_$mimesha1";
    my $mimeboundaryalt = "mime_imessage_alt_$mimesha1";

    my $has_attachments = scalar(@output_attachments) > 0;
    my $is_mime = $allow_html || $has_attachments;
    my $content_type_mixed = "multipart/mixed; boundary=\"$mimeboundarymixed\"";
    my $content_type_alt = $allow_html ? "multipart/alternative; boundary=\"$mimeboundaryalt\"; charset=\"utf-8\"" : 'text/plain; charset="utf-8"';
    my $initial_content_type = $has_attachments ? $content_type_mixed : $content_type_alt;

    print TMPEMAIL <<EOF
Return-Path: <$imessageuser>
Delivered-To: $imessageuser
From: $imessageuserlongname <$imessageuser>
To: $imessageuserlongname <$imessageuser>
Date: $emaildate
Subject: Chat with $outperson at $localdate ...
MIME-Version: 1.0
Content-Type: $initial_content_type
Content-Transfer-Encoding: binary
X-Mailer: archive_imessage.pl $VERSION

EOF
;

    if ($is_mime) {
        print TMPEMAIL "This is a multipart message in MIME format.\n\n";
    }

    if (@output_attachments) {
        print TMPEMAIL <<EOF
--$mimeboundarymixed
Content-Type: $content_type_alt
Content-Transfer-Encoding: binary

EOF
;
    }

    if (not $allow_html) {
        print TMPEMAIL "$output_text";
        print TMPEMAIL "\n\n";
    } else {
        print TMPEMAIL <<EOF
--$mimeboundaryalt
Content-Type: text/plain; charset="utf-8"
Content-Transfer-Encoding: binary

$output_text

--$mimeboundaryalt
Content-Type: text/html; charset="utf-8"
Content-Transfer-Encoding: binary

<html><head><title>Chat with $outperson at $localdate ...</title><style>
body {
  font-family: "Helvetica Neue";
  font-size: 20px;
  font-weight: normal;
}

section {
  max-width: 450px;
  margin: 50px auto;
}
section div {
  max-width: 255px;
  word-wrap: break-word;
  margin-bottom: 10px;
  line-height: 24px;
}

.clear {
  clear: both;
}

.from-me {
  position: relative;
  padding: 10px 20px;
  color: white;
  background: #0B93F6;
  border-radius: 25px;
  float: right;
}
.from-me:before {
  content: "";
  position: absolute;
  z-index: -1;
  bottom: -2px;
  right: -7px;
  height: 20px;
  border-right: 20px solid #0B93F6;
  border-bottom-left-radius: 16px 14px;
  transform: translate(0, -2px);
}
.from-me:after {
  content: "";
  position: absolute;
  z-index: 1;
  bottom: -2px;
  right: -56px;
  width: 26px;
  height: 20px;
  background: white;
  border-bottom-left-radius: 10px;
  transform: translate(-30px, -2px);
}
.sms {
  background: #04D74A;
}
.sms:before {
  border-right: 20px solid #04D74A;
}
.from-them {
  position: relative;
  padding: 10px 20px;
  background: #E5E5EA;
  border-radius: 25px;
  color: black;
  float: left;
}
.from-them:before {
  content: "";
  position: absolute;
  z-index: 2;
  bottom: -2px;
  left: -7px;
  height: 20px;
  border-left: 20px solid #E5E5EA;
  border-bottom-right-radius: 16px 14px;
  transform: translate(0, -2px);
}
.from-them:after {
  content: "";
  position: absolute;
  z-index: 3;
  bottom: -2px;
  left: 4px;
  width: 26px;
  height: 20px;
  background: white;
  border-bottom-right-radius: 10px;
  transform: translate(-30px, -2px);
}
</style></head><body><section>
$output_html</section></body></html>

--$mimeboundaryalt--

EOF
;

    }

    if (@output_attachments) {
        my %used_fnames = ();
        while (@output_attachments) {
            my $fname = shift @output_attachments;
            my $mimetype = shift @output_attachments;

            my $fdata = undef;
            $fname =~ s#\A\~/##;
            $fname =~ s#\A/var/mobile/##;
            slurp_archived_file('MediaDomain', $fname, \$fdata);

            $fname =~ s#\A.*/##;
            my $tmpfname = $fname;
            my $counter = 0;
            while (defined $used_fnames{$tmpfname}) {
                $counter++;
                $tmpfname = "$counter-$fname";
            }
            $fname = $tmpfname;
            $used_fnames{$fname} = 1;

            print TMPEMAIL <<EOF
--$mimeboundarymixed
Content-Disposition: attachment; filename="$fname"
Content-Type: $mimetype
Content-Transfer-Encoding: base64

EOF
;

            print TMPEMAIL encode_base64($fdata);
            print TMPEMAIL "\n";
            $fdata = undef;
        }

        print TMPEMAIL "--$mimeboundarymixed--\n\n";
    }

    close(TMPEMAIL);

    $output_text = undef;
    $output_html = undef;
    @output_attachments = ();

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

sub split_date_time {
    my $timestamp = shift;
    my $date = strftime('%Y-%m-%d', localtime($timestamp));
    my $time = strftime('%H:%M', localtime($timestamp));
    dbgprint("split $timestamp => '$date', '$time'\n");
    return ($date, $time);
}


# don't care if these fail.
mkdir("$maildir", 0700);
mkdir("$maildir/tmp", 0700);
mkdir("$maildir/cur", 0700);
mkdir("$maildir/new", 0700);

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

my $dbname = archive_fname('HomeDomain', 'Library/SMS/sms.db');
dbgprint("message database is '$dbname'\n");
my $db = DBI->connect("DBI:SQLite:dbname=$dbname", '', '', { RaiseError => 0 })
    or fail("Couldn't open message database at '$archivedir/$dbname': " . $DBI::errstr);

my $addressbookdbname = archive_fname('HomeDomain', 'Library/AddressBook/AddressBook.sqlitedb');
dbgprint("address book database is '$addressbookdbname'\n");
my $addressbookdb = DBI->connect("DBI:SQLite:dbname=$addressbookdbname", '', '', { RaiseError => 0 })
    or fail("Couldn't open addressbook database at '$archivedir/$addressbookdbname': " . $DBI::errstr);

my $stmt;

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

$imessageuserhost = $imessageuser;
$imessageuserhost =~ s/\A.*\@//;

dbgprint("longname for imessageuser ($imessageuser) == " . $longnames{-1} . "\n");
dbgprint("shortname for imessageuser ($imessageuser) == " . $shortnames{-1} . "\n");
dbgprint("imessageuserhost == $imessageuserhost\n");

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

$stmt = $db->prepare('select h.id, m.ROWID, m.text, m.service, m.account, m.handle_id, m.subject, m.date, m.is_emote, m.is_from_me, m.was_downgraded, m.is_audio_message, m.cache_has_attachments from message as m inner join handle as h on m.handle_id=h.ROWID order by m.ROWID;')
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

    my ($idname, $msgid, $text, $service, $account, $handle_id, $subject, $date, $is_emote, $is_from_me, $was_downgraded, $is_audio_message, $cache_has_attachments) = @row;
    next if not defined $text;

    # Convert from Cocoa epoch to Unix epoch (2001 -> 1970).
    $date += 978307200;

    # !!! FIXME: do something if defined $subject
    # !!! FIXME: do something with $service

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

        $outtimestamp = $date;
        $outhandle_id = $handle_id;
        $outid = $msgid;

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

        $outperson = $idname;
        if ($outperson ne $fullalias) {
            $outperson = "$fullalias [$idname]";
        }

        $lasthandle_id = $handle_id;
        $lastspeaker = '';
        $lastdate = 0;
        $lastday = '';

        $output_text = '';
        $output_html = '';
        @output_attachments = ();
    }

    # UTF-8 for non-breaking space (&nbsp;). Dump it at end of line; iMessage seems to add it occasionally (maybe double-space to add a period then hit Send?).
    $text =~ s/\xC2\xA0\Z//;

    # replace "/me does something" with "*does something*" ...
    $text =~ s#\A/me (.*)\Z#*$1*#m;

    my $plaintext = $text;
    my $htmltext = $text;

    # Strip out the usual suspects.
    $htmltext =~ s/\&/&amp;/g;
    $htmltext =~ s/\</&lt;/g;
    $htmltext =~ s/\>/&gt;/g;
    $htmltext =~ s#\n#<br/>\n#g;

    if ($is_emote) {
        $htmltext = "<b>$htmltext</b>";
    }

    if ($cache_has_attachments) {
        $attachmentstmt->execute($msgid) or fail("Couldn't execute attachment lookup SELECT statement: " . $DBI::errstr);
        while (my @attachmentrow = $attachmentstmt->fetchrow_array()) {
            my ($fname, $mimetype) = @attachmentrow;
            push @output_attachments, $fname;
            push @output_attachments, $mimetype;

            my $shortfname = $fname;
            $shortfname =~ s/\A.*\///;
            $plaintext =~ s/\xEF\xBF\xBC/\n[attachment $shortfname]\n/;

            if ($allow_html) {
                # !!! FIXME: videos need a thumbnail image to scale.
                if ($mimetype =~ /\Aimage\//) {
                    my $fnameimg = $fname;
                    $fnameimg =~ s#\A\~/##;
                    $fnameimg =~ s#\A/var/mobile/##;
                    my $hashedfname = archive_fname('MediaDomain', $fnameimg);
                    $fnameimg =~ s#.*/##;
                    my $outfname = "$maildir/tmp/imessage-chatlog-tmp-$$-$msgid-$fnameimg";
                    my $cmdline = "sips --resampleWidth 235 '$hashedfname' --out '$outfname' 1>/dev/null 2>/dev/null";
                    dbgprint("resize image: $cmdline\n");
                    die('sips failed') if (system($cmdline) != 0);
                    my $fdata = undef;
                    if ((not defined read_file($outfname, buf_ref => \$fdata, binmode => ':raw')) or (not defined $fdata)) {
                        unlink($outfname);
                        fail("Couldn't read attachment '$outfname': $!");
                    }
                    unlink($outfname);
                    my $base64 = encode_base64($fdata);
                    $fdata = undef;
                    $htmltext =~ s#\xEF\xBF\xBC#<img src='data:$mimetype;base64,$base64'/><br/>\n#;
                    $base64 = undef;
                } else {
                    $htmltext =~ s#\xEF\xBF\xBC#[attachment $shortfname]<br/>\n#;
                }
            }

            dbgprint("ATTACHMENT! fname=$fname shortfname=$shortfname mime=$mimetype\n");
        }
    }

    my $speaker = $is_from_me ? $thisimessageuseralias : $alias;
    #$speaker .= " [$service]" if ($service ne 'iMessage');

    my ($d, $t) = split_date_time($date);

    if ((defined $lastday) and ($lastday ne $d)) {
        $output_text .= "\n$d\n";
        $output_html .= "<p align='center'>$d</p>\n" if ($allow_html);
        $lastspeaker = '';  # force it to redraw in the text output.
    }

    if ($allow_html) {
        my $htmlfromclass;
        if ($is_from_me) {
            $htmlfromclass = ($service ne 'SMS') ? 'from-me' : 'from-me sms';
        } else {
            $htmlfromclass = 'from-them';
        }

        $output_html .= "<div title='$d, $t' class='$htmlfromclass'>$htmltext</div><div class='clear'></div>\n";
    }

    $output_text .= "\n$speaker:\n" if ($lastspeaker ne $speaker);
    $output_text .= "$t  $plaintext\n";

    $lastdate = $date;
    $lastday = $d;
    $lastspeaker = $speaker;
    $outmsgid = $msgid;
}

$db->disconnect();
exit(0);

# end of archive_imessage.pl ...

