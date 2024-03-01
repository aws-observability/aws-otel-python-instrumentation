import http from "k6/http";
import {check} from "k6";

const baseUri = 'http://vehicle-service:8001/vehicle-inventory/';
// Fake ID to trigger 400's.
const badId = 1000;

export default function() {
    /**
     * Calls all Vehicle Inventory Service APIs.
     *
     * Note that there are no modifications to the database - all POSTs and DELETEs will fail.
     * This ensures the database does not grow over time. Data is initially added in setUp.js.
     */

    invokeApi("oops", "GET", 404)
    invokeApi("", "GET", 200)
    invokeApi("", "POST", 400)
    invokeApi("1", "GET", 200)
    invokeApi(`${badId}`, "DELETE", 404)
    invokeApi("make/Toyota", "GET", 200)
    invokeApi("1/image", "GET", 200)
    invokeApi("image/toy_rav_24.png", "GET", 200)
    invokeApi("image/", "POST", 404)
    invokeApi("history/", "GET", 200)
    invokeApi("history/", "POST", 400)
    invokeApi("history/1", "GET", 200)
    invokeApi(`history/${badId}`, "DELETE", 404)
    invokeApi("history/1/vehicle", "GET", 200)

    function invokeApi(path, method, status) {
        const url = `${baseUri}${path}`;
        let response;
        switch(method) {
            case "GET":
                response = http.get(url);
                break;
            case "POST":
                response = http.post(url, JSON.stringify({"badKey": "badValue"}));
                break;
            case "DELETE":
                response = http.del(url);
                break;
        }
        check(response, {
            [`${method} ${path} response ${status}`] : (response) => response.status === status
        });
    }
};