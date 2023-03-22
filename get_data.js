const request = require("request");

// TO-DO
// function jsonToCsv(data) {
//     const json_data = data;

//     const titles = Object.keys(json_data[0]);
//     let csv_string = "";

//     for(let i = 0; i < titles.length; i++)
//         csv_string += titles[i];
    
//     csv_string += '\n';


// };

const queries = {
    market: "KRW-BTC",
    to: "",
    count: "200",
};

const url = new URL("https://api.upbit.com/v1/candles/days");
url.search = new URLSearchParams(queries);

const options = {
    method: "GET",
    url: url.toString(),
    headers: {accept: "application/json"}
};


request(options, (err, resp, body) => {
    if(err) throw new Error(err);

    else if(resp.statusCode != 200) {
        console.error("unable to get data(status != 200)");
    }

    data = JSON.parse(body);
    // csv_file = jsonToCsv(data);
});