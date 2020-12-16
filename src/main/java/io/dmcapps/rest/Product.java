package io.dmcapps.rest;

import io.quarkus.hibernate.orm.panache.PanacheEntity;

public class Product extends PanacheEntity {

    public String name;
    public Brand birth;
    public Category status;
    public String picture;
    
}
