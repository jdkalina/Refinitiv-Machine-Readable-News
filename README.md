# Refinitiv-Machine-Readable-News
Compare Machine Readable archive to historical Tick Database to determine how many profitable and exclusive news stories are released by Reuters.

Files from MRN were sourced using command line ftp function from Refinitiv's MRN Headline Direct FTP service. Headline Direct is a low latency Machine Readable news feed that gives access to breaking Reuters news in Realtime.

Next processing and transforming of the files is done within the mrnProcessing.py file methods.

That output is passed to the tickFiles.py methods which query Refinitiv's historical Tick REST api for the intraday updates on RICS (Reuters Instrument Codes).

That output is then paired with the chartingOutput.py methods to create charts for each breaking Reuters exclusive. An example of the output is shown in picture0.jpg.
