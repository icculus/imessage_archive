#!/usr/bin/perl -w

use warnings;
use strict;

use File::Basename;
use Digest::SHA1 qw(sha1_hex);
use DBI;
use Encode qw( decode_utf8 );
use POSIX;
use File::Copy;
use MIME::Base64;
use File::Slurp;
use Regexp::Common qw/URI/;

my $VERSION = '0.0.1';

my $gaptime = (30 * 60);
my $timezone = strftime('%Z', localtime());
my $now = time();
my $homedir = $ENV{'HOME'};
my $program_dir = dirname($0);


# Fixes unicode dumping to stdio...hopefully you have a utf-8 terminal by now.
#binmode(STDOUT, ":utf8");
#binmode(STDERR, ":utf8");

sub fail {
    my $err = shift;
    die("$err\n");
}

sub signal_catcher {
    my $sig = shift;
    fail("Caught signal ${sig}!");
}
$SIG{INT} = \&signal_catcher;
$SIG{TERM} = \&signal_catcher;
$SIG{HUP} = \&signal_catcher;

my $debug = 0;
sub dbgprint {
    print @_ if $debug;
}

my $archivedir = undef;
my $imessageuser = undef;
my $imessageuserhost = undef;
my $imessageuserlongname = undef;
my $imessageusershortname = undef;
my $maildir = undef;
my $allow_html = 0;
my $report_progress = 0;
my $attachment_shrink_percent = undef;
my $allow_video_attachments = 1;
my $allow_attachments = 1;
my $allow_thumbnails = 1;
my $ios_archive = 0;

sub usage {
    print STDERR "USAGE: $0 [...options...] <backupdir> <maildir>\n";
    print STDERR "\n";
    print STDERR "    --debug: Enable spammy debug logging to stdout.\n";
    print STDERR "    --html: Output HTML archives.\n";
    print STDERR "    --progress: Print progress to stdout.\n";
    print STDERR "    --attachments-shrink-percent=NUM: resize videos/images to NUM percent.\n";
    print STDERR "    --no-video-attachments: Don't include image/video attachments.\n";
    print STDERR "    --no-attachments: Don't include attachments at all.\n";
    print STDERR "    --no-thumbnails: Don't include thumbnails in HTML output.\n";
    print STDERR "    --gap-time=NUM: treat NUM minutes of silence as the end of a conversation.\n";
    print STDERR "    backupdir: Directory holding unencrypted iPhone backup.\n";
    print STDERR "    maildir: Path of Maildir where we write archives and metadata.\n";
    print STDERR "\n";
    exit(1);
}

foreach (@ARGV) {
    $debug = 1, next if $_ eq '--debug';
    $debug = 0, next if $_ eq '--no-debug';
    $allow_html = 1, next if $_ eq '--html';
    $allow_html = 0, next if $_ eq '--no-html';
    $report_progress = 1, next if $_ eq '--progress';
    $report_progress = 0, next if $_ eq '--no-progress';
    $allow_video_attachments = 1, next if $_ eq '--video-attachments';
    $allow_video_attachments = 0, next if $_ eq '--no-video-attachments';
    $allow_attachments = 1, next if $_ eq '--attachments';
    $allow_attachments = 0, next if $_ eq '--no-attachments';
    $allow_thumbnails = 1, next if $_ eq '--thumbnails';
    $allow_thumbnails = 0, next if $_ eq '--no-thumbnails';
    $attachment_shrink_percent = int($1), next if /\A--attachments-shrink-percent=(\d+)\Z/;
    $gaptime = int($1) * 60, next if /\A--gap-time=(\d+)\Z/;
    $archivedir = $_, next if not defined $archivedir;
    $maildir = $_, next if (not defined $maildir);
    usage();
}
usage() if not defined $archivedir;
usage() if not defined $maildir;

if ((defined $attachment_shrink_percent) && ($attachment_shrink_percent == 100)) {
    $attachment_shrink_percent = undef;
} elsif ((defined $attachment_shrink_percent) && (($attachment_shrink_percent < 1) || ($attachment_shrink_percent > 100))) {
    fail("--attachments-shrink-percent must be between 1 and 100.");
}

sub archive_fname {
    my $domain = shift;
    my $name = shift;
    if ($ios_archive) {
        my $combined = "$domain-$name";
        my $hashed = sha1_hex($combined);
        dbgprint("Hashed archived filename '$combined' to '$hashed'\n");
        return "$archivedir/$hashed";
    }

    return "$archivedir/$name";
}


fail("ERROR: Directory '$archivedir' doesn't exist.") if (not -d $archivedir);
if (-f "$archivedir/Manifest.mbdb") {
    $ios_archive = 1;
} elsif (-f "$archivedir/chat.db") {
    $ios_archive = 0;
} else {
    fail("ERROR: '$archivedir' isn't a macOS Messages directory or an iOS backup.\n");
}

if ($ios_archive) {
    dbgprint("Chat database is in an iOS backup.\n");
} else {
    dbgprint("Chat database is in a macOS install.\n");
}

my $startid = 0;
my %startids = ();
my $lastarchivefname = undef;
my $lastarchivetmpfname = undef;

sub flush_startid {
    my ($with, $msgid) = @_;

    if (defined $with and defined $msgid) {
        $startids{$with} = $msgid;
        dbgprint("flushing startids (global: $startid, '$with': $msgid)\n");
    } else {
        dbgprint("flushing startids (global: $startid)\n");
    }

    if (not open(LASTID,'>',$lastarchivetmpfname)) {
        dbgprint("Open '$lastarchivetmpfname' for write failed: $!\n");
        return 0;
    }
    print LASTID "$startid\n";
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

    if (not -f $hashedfname) {
        if ($ios_archive) {
            print STDERR "WARNING: Missing attachment '$hashedfname' ('$domain', '$fname')\n";
        } else {
            print STDERR "WARNING: Missing attachment '$hashedfname'\n";
        }
        $$fdataref = '[MISSING]';
        return 0;
    }

    if ((not defined read_file($hashedfname, buf_ref => $fdataref, binmode => ':raw', err_mode => 'carp')) or (not defined $fdataref)) {
        fail("Couldn't read attachment '$hashedfname': $!");
    }

    return 1;
}

sub get_image_orientation {
    my $fname = shift;
    my $cmdline = "$program_dir/exiftool/exiftool -n -Orientation '$fname'";
    my $orientation = `$cmdline`;
    fail("exiftool failed ('$cmdline')") if ($? != 0);
    chomp($orientation);
    $orientation =~ s/\AOrientation\s*\:\s*(\d*)\s*\Z/$1/;
    dbgprint("File '$fname' has an Orientation of '$orientation'.\n");
    return ($orientation eq '') ? undef : int($orientation);
}

sub set_image_orientation {
    my $fname = shift;
    my $orientation = shift;
    my $trash = shift;
    if (defined $orientation) {
        my $cmdline = "$program_dir/exiftool/exiftool -q -n -Orientation=$orientation '$fname'";
        dbgprint("marking image orientation: $cmdline\n");
        if (system($cmdline) != 0) {
            unlink($fname) if $trash;
            fail("exiftool failed ('$cmdline')")
        }
    }
}

sub load_attachment {
    my $origfname = shift;
    my $hashedfname = shift;
    my $fdataref = shift;
    my $mimetype = shift;

    my $is_image = $mimetype =~ /\Aimage\//;
    my $is_video = $mimetype =~ /\Avideo\//;
    if ((defined $attachment_shrink_percent) && ($is_image || $is_video)) {
        my $is_jpeg = $mimetype eq 'image/jpeg';
        my $fmt = $is_jpeg ? '-f mjpeg' : '';
        my $orientation = get_image_orientation($hashedfname);
        my $fract = $attachment_shrink_percent / 100.0;
        my $basefname = $origfname;
        $basefname =~ s#.*/##;
        my $outfname = "$maildir/tmp/imessage-chatlog-tmp-$$-attachment-shrink-$basefname";
        my $cmdline = "$program_dir/ffmpeg $fmt -i '$hashedfname' -vf \"scale='trunc(iw*$fract)+mod(trunc(iw*$fract),2)':'trunc(ih*$fract)+mod(trunc(ih*$fract),2)'\" '$outfname' 2>/dev/null";
        dbgprint("shrinking attachment: $cmdline\n");
        die("ffmpeg failed ('$cmdline')") if (system($cmdline) != 0);
        set_image_orientation($outfname, $orientation, 1);
        read_file($outfname, buf_ref => $fdataref, binmode => ':raw', err_mode => 'carp');
        unlink($outfname);
    } else {
        read_file($hashedfname, buf_ref => $fdataref, binmode => ':raw', err_mode => 'carp');
    }
}


my %longnames = ();
my %shortnames = ();

# !!! FIXME: this should probably go in a hash or something.
my $outperson = undef;
my $outmsgid = undef;
my $outhandle_id = undef;
my $outid = undef;
my $outtimestamp = undef;
my $outhasfromme = 0;
my $outhasfromthem = 0;
my $outhasfromme_a = 0;
my $outhasfromme_sms = 0;
my $outhasfromme_sms_a = 0;
my $output_text = '';
my $output_html = '';
my @output_attachments = ();

sub flush_conversation {
    return if (not defined $outmsgid);

    my $trash = shift;

    dbgprint("Flushing conversation! trash=$trash\n");

    if ($trash) {
        $output_text = '';
        $output_html = '';
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
        # This <style> section is largely based on:
        #   https://codepen.io/samuelkraft/pen/Farhl/
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
EOF
;

        # Don't output parts of the CSS we don't need to save a little space.
        if ($outhasfromme) {
            print TMPEMAIL <<EOF
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
EOF
;
        }

        if ($outhasfromme_a) {
            print TMPEMAIL <<EOF
.from-me a {
  color: white;
  background: #0B93F6;
}
EOF
;
        }

        if ($outhasfromme_sms) {
            print TMPEMAIL <<EOF
.sms {
  background: #04D74A;
}
.sms:before {
  border-right: 20px solid #04D74A;
}
EOF
;
        }

        if ($outhasfromme_sms_a) {
            print TMPEMAIL <<EOF
.sms a {
  color: white;
  background: #04D74A;
}
EOF
;
        }

        if ($outhasfromthem) {
            print TMPEMAIL <<EOF
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
EOF
;
        }

        print TMPEMAIL <<EOF
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
            my $domain = 'MediaDomain';

            $fname =~ s#\A\~/#$homedir/# if (not $ios_archive);
            $fname =~ s#\A\~/## if ($ios_archive);
            $fname =~ s#\A/var/mobile/## if ($ios_archive);
            my $hashedfname = archive_fname($domain, $fname);

            my $fdata = undef;
            if (not -f $hashedfname) {
                if ($ios_archive) {
                    print STDERR "WARNING: Missing attachment '$hashedfname' ('$domain', '$fname')\n";
                } else {
                    print STDERR "WARNING: Missing attachment '$hashedfname'\n";
                }
            } else {
                load_attachment($fname, $hashedfname, \$fdata, $mimetype);
                if ($ios_archive) {
                    print STDERR "WARNING: Failed to load '$hashedfname' ('$domain', '$fname')\n" if (not defined $fdata);
                } else {
                    print STDERR "WARNING: Failed to load '$hashedfname'\n" if (not defined $fdata);
                }
            }

            $fname =~ s#\A.*/##;
            my $tmpfname = $fname;
            my $counter = 0;
            while (defined $used_fnames{$tmpfname}) {
                $counter++;
                $tmpfname = "$counter-$fname";
            }
            $fname = $tmpfname;
            $used_fnames{$fname} = 1;

            if (not defined $fdata) {
                print TMPEMAIL <<EOF
--$mimeboundarymixed
Content-Disposition: attachment; filename="$fname"
Content-Type: text/plain; charset="utf-8"
Content-Transfer-Encoding: binary

This file was missing in the iPhone backup (or there was a bug) when this
archive was produced. Sorry!

EOF
;
            } else {
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
        }

        print TMPEMAIL "--$mimeboundarymixed--\n\n";
    }

    close(TMPEMAIL);

    $output_text = '';
    $output_html = '';
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

my $stmt;

my $dbname = archive_fname('HomeDomain', $ios_archive ? 'Library/SMS/sms.db' : 'chat.db');
dbgprint("message database is '$dbname'\n");
my $db = DBI->connect("DBI:SQLite:dbname=$dbname", '', '', { RaiseError => 0 })
    or fail("Couldn't open message database at '$archivedir/$dbname': " . $DBI::errstr);

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

my $addressbookdbname = undef;  # only used on iOS archives right now.
my $addressbookdb = undef;  # only used on iOS archives right now.
my $lookupstmt = undef;  # only used on iOS archives right now.

if ($ios_archive) {
    $addressbookdbname = archive_fname('HomeDomain', 'Library/AddressBook/AddressBook.sqlitedb');
    dbgprint("address book database is '$addressbookdbname'\n");
    $addressbookdb = DBI->connect("DBI:SQLite:dbname=$addressbookdbname", '', '', { RaiseError => 0 })
        or fail("Couldn't open addressbook database at '$archivedir/$addressbookdbname': " . $DBI::errstr);

    $lookupstmt = $addressbookdb->prepare('select c15Phone, c16Email, c11Nickname, c0First, c2Middle, c1Last from ABPersonFullTextSearch_content where ((c15Phone LIKE ?) or (c16Email LIKE ?)) limit 1;')
        or fail("Couldn't prepare name lookup SELECT statement: " . $DBI::errstr);
}

sub lookup_ios_address {
    my $address = shift;
    my $like = "%$address%";

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
    return @lookuprow;
}


my %mac_addressbook = ();
if (not $ios_archive) {
    %mac_addressbook = ();
    open(HELPERIO, '-|', "./dump_mac_addressbook") or die("Can't run ./dump_mac_addressbook: $!\n");

    my @lines = ();
    while (<HELPERIO>) {
        chomp;
        dbgprint("dump_mac_addressbook line: '$_'\n");
        push @lines, $_;
    }
    close(HELPERIO);

    my @person = ();
    while (@lines) {
        for (my $i = 0; $i < 4; $i++ ) {
            my $x = shift @lines;
            last if not defined $x;
            push @person, $x eq '' ? undef : $x;
        }

        if (scalar(@person) != 4) {
            dbgprint("incomplete record from dump_mac_addressbook!\n");
            last;
        } elsif ($debug) {
            my ($a, $b, $c, $d) = @person;
            $a = '' if not defined $a;
            $b = '' if not defined $b;
            $c = '' if not defined $c;
            $d = '' if not defined $d;
            dbgprint("Person from dump_mac_addressbook: [$a] [$b] [$c] [$d]\n");
        }

        # Phone numbers...flatten them down.
        while (@lines) {
            my $x = shift @lines;
            last if (not defined $x) || ($x eq '');
            $x =~ s/[^0-9]//g;  # flatten.
            $mac_addressbook{$x} = [ @person ];
            dbgprint("Person phone: [$x]\n");
        }

        # Emails...lowercase them.
        while (@lines) {
            my $x = shift @lines;
            last if (not defined $x) || ($x eq '');
            $mac_addressbook{lc($x)} = [ @person ];
            dbgprint("Person email: [$x]\n");
        }

        @person = ();
    }

    dbgprint("Done pulling in Mac address book.\n");
}

sub lookup_macos_address {
    my $address = shift;

    # !!! FIXME: this all sucks.
    my $phone = $address;
    $phone =~ s/\A\+1//;
    $phone =~ s/[^0-9]//g;  # flatten.
    my $email = lc($address);

    my @lookuprow = ();
    foreach (keys(%mac_addressbook)) {
        if ( (($email ne '') && (index($_, $email) != -1)) ||
             (($phone ne '') && (index($_, $phone) != -1)) ) {
            my $person = $mac_addressbook{$_};
            @lookuprow = @$person;
            last;
        }
    }

    while (scalar(@lookuprow) < 4) {
        push @lookuprow, undef;
    }

    return @lookuprow;
}

sub lookup_address {
    my ($handleid, $address) = @_;
    dbgprint("looking for $address...\n");
    my @lookuprow = $ios_archive ? lookup_ios_address($address) : lookup_macos_address($address);
    ($longnames{$handleid}, $shortnames{$handleid}) = parse_addressbook_name($address, @lookuprow);
    dbgprint("longname for $address ($handleid) == " . $longnames{$handleid} . "\n");
    dbgprint("shortname for $address ($handleid) == " . $shortnames{$handleid} . "\n");
}

$stmt = $db->prepare('select m.handle_id, h.id from handle as h inner join (select distinct handle_id from message) m on m.handle_id=h.ROWID;')
    or fail("Couldn't prepare distinct address SELECT statement: " . $DBI::errstr);
$stmt->execute() or fail("Couldn't execute distinct address SELECT statement: " . $DBI::errstr);

while (my @row = $stmt->fetchrow_array()) {
    lookup_address(@row);
}

$stmt = $db->prepare('select distinct account from message;')
    or fail("Couldn't prepare distinct account SELECT statement: " . $DBI::errstr);
$stmt->execute() or fail("Couldn't execute distinct address SELECT statement: " . $DBI::errstr);

my $default_account = undef;
while (my @row = $stmt->fetchrow_array()) {
    my $account = shift @row;
    next if not defined $account;
    my $address = $account;
    $address =~ s/\A[ep]\://i or fail("Unexpected account format '$account'");
    next if $address eq '';
    dbgprint("distinct account: '$account' -> '$address'\n");
    lookup_address($account, $address);
    $default_account = $account if not defined $default_account;  # oh well.
}

if (not defined $default_account) {
    dbgprint("Ugh, forcing a default account.\n");
    $default_account = 'e:donotreply@icloud.com';  # oh well.
    $longnames{$default_account} = 'Unknown User';
    $shortnames{$default_account} = 'Unknown';
}

%mac_addressbook = ();  # dump all this, we're done with it.

$lookupstmt = undef;
$addressbookdb->disconnect() if (defined $addressbookdb);
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
my $thisimessageuseralias = undef;
my $startmsgid = undef;
my $newestmsgid = 0;

my $donerows = 0;
my $totalrows = undef;
my $percentdone = -1;

$stmt = $db->prepare('select ROWID from message order by ROWID desc limit 1;') or fail("Couldn't prepare row count SELECT statement: " . $DBI::errstr);
$stmt->execute() or fail("Couldn't execute row count SELECT statement: " . $DBI::errstr);
my $ending_startid = $startid;
if (my @row = $stmt->fetchrow_array()) {
    $ending_startid = $row[0];
}

if ($report_progress) {
    $stmt = $db->prepare('select count(*) from message where (ROWID > ?)') or fail("Couldn't prepare message count SELECT statement: " . $DBI::errstr);
    $stmt->execute($startid) or fail("Couldn't execute message count SELECT statement: " . $DBI::errstr);
    my @row = $stmt->fetchrow_array();
    $totalrows = $row[0];
}

# We filter to iMessage and SMS, in case some other iChat service landed in here too.
$stmt = $db->prepare("select h.id, m.ROWID, m.text, m.service, m.account, m.handle_id, m.subject, m.date, m.is_emote, m.is_from_me, m.was_downgraded, m.is_audio_message, m.cache_has_attachments from message as m inner join handle as h on m.handle_id=h.ROWID where (m.ROWID > ?) and (m.service='iMessage' or m.service='SMS') order by m.handle_id, m.ROWID;")
    or fail("Couldn't prepare message SELECT statement: " . $DBI::errstr);

my $attachmentstmt = $db->prepare('select filename, mime_type from attachment as a inner join (select rowid,attachment_id from message_attachment_join where message_id=?) as j where a.ROWID=j.attachment_id order by j.ROWID;')
    or fail("Couldn't prepare attachment lookup SELECT statement: " . $DBI::errstr);

$stmt->execute($startid) or fail("Couldn't execute message SELECT statement: " . $DBI::errstr);

while (my @row = $stmt->fetchrow_array()) {
    if ($debug) {
        dbgprint("New row:\n");
        foreach(@row) {
            dbgprint(defined $_ ? "  $_\n" : "  [undef]\n");
        }
    }

    if ($report_progress) {
        my $newpct = int(($donerows / $totalrows) * 100.0);
        if ($newpct != $percentdone) {
            $percentdone = $newpct;
            print("Processed $donerows messages of $totalrows ($percentdone%)\n");
        }
    }
    $donerows++;

    my ($idname, $msgid, $text, $service, $account, $handle_id, $subject, $date, $is_emote, $is_from_me, $was_downgraded, $is_audio_message, $cache_has_attachments) = @row;
    next if not defined $text;

    # Convert from Cocoa epoch to Unix epoch (2001 -> 1970).
    $date += 978307200;

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
        if ($msgid < $ending_startid) {
            $ending_startid = ($startmsgid-1);
            dbgprint("forcing global startid to $ending_startid\n");
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

        $account = $default_account if (not defined $account); # happens on old SMS messages.
        $account = $default_account if $account =~ /\A[ep]\:\Z/i;

        $imessageuser = $account;
        if ($imessageuser =~ s/\A([ep])\://i) {
            my $type = lc($1);
            if ($type eq 'p') {
                $imessageuser =~ s/[^\d]//g;
                $imessageuser = "phone-$imessageuser\@icloud.com";
            }
        } else {
            fail("BUG: this shouldn't have happened."); # we checked this before.
        }

        $imessageuserlongname = $longnames{$account};
        $imessageusershortname = $shortnames{$account};

        $imessageuserhost = $imessageuser;
        $imessageuserhost =~ s/\A.*\@//;

        dbgprint("longname for imessageuser ($imessageuser) == $imessageuserlongname\n");
        dbgprint("shortname for imessageuser ($imessageuser) == $imessageusershortname\n");
        dbgprint("imessageuserhost == $imessageuserhost\n");

        $outtimestamp = $date;
        $outhandle_id = $handle_id;
        $outid = $msgid;

        $startmsgid = $msgid;

        my $fullalias = $longnames{$handle_id};
        my $shortalias = $shortnames{$handle_id};

        if ($shortalias ne $imessageusershortname) {
            $alias = $shortalias;
            $thisimessageuseralias = $imessageusershortname;
        } elsif ($fullalias ne $imessageuserlongname) {
            $alias = $fullalias;
            $thisimessageuseralias = $imessageuserlongname;
        } else {
            $alias = "$shortalias ($idname)";
            $thisimessageuseralias = "$imessageusershortname ($imessageuser)";
        }

        $outperson = $idname;
        if ($outperson ne $fullalias) {
            $outperson = "$fullalias [$idname]";
        }

        $lasthandle_id = $handle_id;
        $lastspeaker = '';
        $lastdate = 0;
        $lastday = '';

        $outhasfromme = 0;
        $outhasfromthem = 0;
        $outhasfromme_a = 0;
        $outhasfromme_sms = 0;
        $outhasfromme_sms_a = 0;

        $output_text = '';
        $output_html = '';
        @output_attachments = ();

        if (defined $subject) {
            chomp($subject);
            $subject =~ s/\A\s+//;
            $subject =~ s/\s+\Z//;
            if (($subject eq '') || ($subject eq 'Re:')) {
                $subject = undef;
            }
        }

        if (defined $subject) {
            $output_text = "iMessage subject: $subject\n\n";
            $output_html = "<p><i>$subject</i></p>\n\n";
        }
    }

    my $is_sms = (defined $service) && ($service eq 'SMS');

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

    if ($is_from_me) {
        $outhasfromme = 1;
        if ($is_sms) {
            $outhasfromme_sms = 1;
        }
    } else {
        $outhasfromthem = 1;
    }

    if ($htmltext =~ s/($RE{URI})/<a href="$1">$1<\/a>/g) {
        if ($is_from_me) {
            if ($is_sms) {
                $outhasfromme_sms_a = 1;
            } else {
                $outhasfromme_a = 1;
            }
        }
    }

    if ($cache_has_attachments) {
        $attachmentstmt->execute($msgid) or fail("Couldn't execute attachment lookup SELECT statement: " . $DBI::errstr);
        while (my @attachmentrow = $attachmentstmt->fetchrow_array()) {
            my ($fname, $mimetype) = @attachmentrow;

            my $is_image = $mimetype =~ /\Aimage\//;
            my $is_video = $mimetype =~ /\Avideo\//;

            if (($allow_attachments) && ($allow_video_attachments || (not ($is_video || $is_image)))) {
                push @output_attachments, $fname;
                push @output_attachments, $mimetype;
            }

            my $shortfname = $fname;
            $shortfname =~ s/\A.*\///;
            $plaintext =~ s/\xEF\xBF\xBC/\n[attachment $shortfname]\n/;

            if ($allow_html) {
                if ($allow_thumbnails && ($is_image || $is_video)) {
                    my $fnameimg = $fname;
                    $fnameimg =~ s#\A\~/#$homedir/# if (not $ios_archive);
                    $fnameimg =~ s#\A\~/## if ($ios_archive);
                    $fnameimg =~ s#\A/var/mobile/## if ($ios_archive);
                    my $domain = 'MediaDomain';
                    my $hashedfname = $ios_archive ? archive_fname($domain, $fnameimg) : $fnameimg;
                    if (not -f $hashedfname) {
                        if ($ios_archive) {
                            print STDERR "WARNING: Missing attachment '$hashedfname' ('$domain', '$fname')\n";
                        } else {
                            print STDERR "WARNING: Missing attachment '$hashedfname'\n";
                        }
                        $htmltext =~ s#\xEF\xBF\xBC#[Missing image '$fnameimg']<br/>\n#;
                    } else {
                        $fnameimg =~ s#.*/##;
                        my $orientation = get_image_orientation($hashedfname);
                        my $outfname = "$maildir/tmp/imessage-chatlog-tmp-$$-$msgid-$fnameimg.jpg";
                        my $is_jpeg = $mimetype eq 'image/jpeg';
                        my $fmt = $is_jpeg ? '-f mjpeg' : '';
                        my $cmdline = "$program_dir/ffmpeg $fmt -i '$hashedfname' -frames 1 -vf 'scale=235:-1' '$outfname' 2>/dev/null";
                        dbgprint("generating thumbnail: $cmdline\n");
                        die("ffmpeg failed ('$cmdline')") if (system($cmdline) != 0);
                        set_image_orientation($outfname, $orientation, 1);
                        my $fdata = undef;
                        if ((not defined read_file($outfname, buf_ref => \$fdata, binmode => ':raw', err_mode => 'carp')) or (not defined $fdata)) {
                            unlink($outfname);
                            print STDERR "Couldn't read scaled attachment '$outfname': $!";
                            $htmltext =~ s#\xEF\xBF\xBC#[Failed to scale image '$fnameimg']<br/>\n#;
                        } else {
                            unlink($outfname);
                            my $base64 = encode_base64($fdata);
                            $fdata = undef;
                            $htmltext =~ s#\xEF\xBF\xBC#<center><img src='data:$mimetype;base64,$base64'/></center><br/>\n#;
                            $base64 = undef;
                        }
                    }
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
            $htmlfromclass = $is_sms ? 'from-me sms' : 'from-me';
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

# Flush the final conversation if it's older than the talk gap.
if (($lastdate > 0) && talk_gap($lastdate, $now)) {
    dbgprint("Flushing last conversation.\n");
    $startid = $ending_startid;  # Just flush this here with the conversation.
    flush_conversation(0);
}

# Update the global startid.
if ($ending_startid != $startid) {
    $startid = $ending_startid;
    flush_startid(undef, undef);
}

if ($report_progress) {
    print("All completed conversations archived.\n");
}

exit(0);

# end of archive_imessage.pl ...
