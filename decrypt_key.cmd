@ echo off

C:\"Program Files"\OpenSSL-Win64\bin\openssl.exe aes-256-cbc -d -salt  -pbkdf2 -in ./keys.enc -out ./keys.json
echo decrypt complete.