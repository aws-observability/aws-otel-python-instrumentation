import http from "k6/http";
import {check} from "k6";

const baseUri = 'http://vehicle-service:8001/vehicle-inventory/';

export default function() {
    postVehicle({'id': 1,'make': 'Toyota', 'model': 'Rav4', 'year': 2024, 'image_name': 'toy_rav_24.png'});
    postVehicle({'id': 2,'make': 'Toyota', 'model': 'Rav4', 'year': 2023, 'image_name': 'toy_rav_23.png'});
    postVehicle({'id': 3,'make': 'Honda', 'model': 'Odyssey', 'year': 2022, 'image_name': 'hon_ody_22.png'});
    postVehicle({'id': 4,'make': 'Honda', 'model': 'Odyssey', 'year': 2024, 'image_name': 'hon_ody_24.png'});

    postVehicleHistory({'id': 1, 'purchase_date': '2024-01-29', 'purchase_price': 55999, 'vehicle_id': 1});
    postVehicleHistory({'id': 2, 'purchase_date': '2024-02-29', 'purchase_price': 54999, 'vehicle_id': 1});
    postVehicleHistory({'id': 3, 'purchase_date': '2023-12-14', 'purchase_price': 13999, 'vehicle_id': 3});

    function postVehicle(body) {
        const response = http.post(baseUri, JSON.stringify(body));
        check(response, {
            [`POST vehicle response 200`] : (response) => response.status === 200
        });
    }

    function postVehicleHistory(body) {
        const response = http.post(`${baseUri}history/`, JSON.stringify(body));
        check(response, {
            [`POST vehicle history response 200`] : (response) => response.status === 200
        });
    }
};