import http from "k6/http";
import {check} from "k6";

const baseUri = `http://vehicle-service:8001/vehicle-inventory/`;

export default function() {

    /**
     * TODO: Test all APIs
     * * Some invalid API
     * * GET /vehicle-inventory/
     * * POST /vehicle-inventory/
     * * GET /vehicle-inventory/<int>
     * * DELETE /vehicle-inventory/<int>
     * * GET /vehicle-inventory/name/<str>
     * * GET /vehicle-inventory/<int>/image
     * * GET /vehicle-inventory/image/<image_name>
     * * POST /vehicle-inventory/image/<image_name>
     * * GET /vehicle-inventory/history/
     * * POST /vehicle-inventory/history/
     * * GET /vehicle-inventory/history/<int>
     * * DELETE /vehicle-inventory/history/<int>
     * * GET /vehicle-inventory/history/<int>/vehicle
     *
     * Below tests are basic and non-comprehensive.
     */

    const inventoryUrl = `${baseUri}`;
    const inventoryResponse = http.get(inventoryUrl);
    check(inventoryResponse, { "getInventory 2XX": r => isStatus2XX(r)});

    const imageUrl = `${baseUri}image/fake-image`;
    const imageResponse = http.get(imageUrl);
    check(imageResponse, { "getImage 4XX": r => isStatus4XX(r)});

    function isStatus2XX (response) {
        return response.status >= 200 && response.status <= 399
    }

    function isStatus4XX (response) {
        return response.status >= 400 && response.status <= 499
    }
};