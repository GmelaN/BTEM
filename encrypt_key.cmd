@ echo off

"C:\Program Files\OpenSSL-Win64\bin\openssl.exe" enc -e -aes-256-cbc -salt -pbkdf2 -in ./keys.json -out ./keys.enc
del "./keys.json"
echo encrypt complete.