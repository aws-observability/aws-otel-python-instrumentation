import http from "k6/http";
import {check} from "k6";

const baseUri = 'http://requests-service:8080/';

export default function() {
    invokeApi("success", "GET", 200)
    invokeApi("error", "GET", 400)
    invokeApi("fault", "GET", 500)
    invokeApi("oops", "GET", 404)

    function invokeApi(path, method, status) {
        const url = `${baseUri}${path}`;
        let response;
        switch(method) {
            case "GET":
                response = http.get(url);
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