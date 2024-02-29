CREATE TABLE IF NOT EXISTS Vehicle (
    id INT,
    make VARCHAR(255),
    model VARCHAR(255),
    year INT,
    image_name VARCHAR(255),
    CONSTRAINT pk_Vehicle PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS VehiclePurchaseHistory (
    id INT,
    purchase_date DATE,
    purchase_price INT,
    vehicle INT,
    FOREIGN KEY (vehicle) REFERENCES Vehicle(id),
    CONSTRAINT pk_VehiclePurchaseHistory PRIMARY KEY (id)
);
