# zenko
* Straightforward, simple sponsored tiles/redshift reporting system
* "Zenko" (善狐)  means good/helpful fox in Japanese: https://en.wikipedia.org/wiki/Kitsune#Characteristics

# Installation:
1. > cd ~/Documents
2. > git clone https://github.com/matthewruttley/zenko.git
 (You may be prompted to install XCode, if so, please do)
3. Open finder and navigate to Documents --> zenko
4. Drag the two files "1. Start Zenko Server" and "2. Open Zenko" to your Desktop
5. Install: http://postgresapp.com/ and drag the application to your Applications folder
6. > PATH=/Applications/Postgres.app/MacOS/Versions/9.4/bin:$PATH
 (the location of the bin folder above may differ)
7. > sudo easy_install flask psycopg2
8. Make sure you have the Mozilla VPN installed (see mana documentation)
9. Make sure you're part of the LDAP group (see the "redshift" bugzilla link at the bottom of this page if not: https://mana.mozilla.org/wiki/display/SVCOPS/Tiles+Architecture#TilesArchitecture-VPNAccess)
10. Email mruttley@mozilla.com for the login script. I can't put it publicly on github.

# Usage:
1. Make sure you're connected to the Mozilla VPN
2. Double click file 1 on your desktop. Wait for it to load the cache.
3. Double click file 2 
4. Have fun exploring data
