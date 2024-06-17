import http from "k6/http";
import {check} from "k6";

export default function() {
    let response = http.get(`http://simple-service:8080/dep`);
    check(response, {
        [`GET dep response 200`] : (response) => response.status === 200
    });
};