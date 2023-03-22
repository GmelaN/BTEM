const request = require('request')
const uuidv4 = require("uuid4")
const sign = require("jsonwebtoken").sign;

// read decrypted key file and load as object
const fs = require("fs")
const fileBuffer = fs.readFileSync("keys.json")
const keys = JSON.parse(fileBuffer.toString())

const server_url = "https://api.upbit.com"
const payload = {
    access_key: keys.access,
    nonce: uuidv4(),
}

const token = sign(payload, keys.secret)

const options = {
    method: "GET",
    url: server_url + "/v1/accounts",
    headers: {"Authorization": `Bearer ${token}`},
}

request(options, (error, response, body) => {
    if (error) throw new Error(error)
    console.log(body)
})