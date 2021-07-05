# imessage_archive

imessage_archive is a Perl script that converts iMessage's database of 
conversations into an email archive. It can process chat messages 
stored in an unencrypted iPhone backup, or those from the macOS
Messages app. It has been tested with an iOS 9 backup generated with
iTunes 12.4 and a Messages install on Mac OS X 10.11, but it might work
on other similar versions.

Archives can be customized in several ways. They can be simple text emails,
or rich HTML emails. Photos and videos are included in the HTML version and
viewed inline, to simulate the Messages app UI. Videos are converted down
to simple animated .gif files. 

You can optionally also keep the original files as email attachments, for a
complete (but much larger) archive.

Options are available to customize your archive. Want it pretty? Use `--html`.
Want to keep the videos but take less space? Try `--attachment-shrink-percent=50`.
Want to keep audio clips and vcards but not all those bulky pictures and 
movies? `--no-video-attachments`. Just want a simple, small, searchable text
archive? `--no-attachments`.

(I personally do `--html --no-video-attachments`, but do whatever fits your needs!)

Email is generated in either Maildir or mboxrd format, depending on whether you
use the `--maildir` or `--mbox` options (default is Maildir). Most email tools
can import and process at least one of these two formats.

This program presumes you are running on a Mac, but can probably be made to work
on other systems with a little effort (such as providing a replacement ffmpeg
binary, etc).

