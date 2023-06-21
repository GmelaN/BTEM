@ echo off

"C:\Program Files\OpenSSL-Win64\bin\openssl.exe" enc -e -aes-256-cbc -salt -pbkdf2 -in ./keys.json -out ./keys.enc
echo encrypt complete.