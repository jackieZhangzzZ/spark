package com.yc.java;

import java.io.Serializable;

public class IpRule implements Serializable {
    private String startIp;
    private String endIp;
    private long startIpInLong;
    private long endIpInLong;
    private String continent;
    private String country;
    private String province;
    private String city;
    private String district;
    private String companyName;
    private String postCode;
    private String countryName;
    private String shortName;
    private double longitude;
    private double latitude;

    //    先创建一个构建方法
    public IpRule(long startIpInLong, long endIpInLong, String province) {
        this.startIpInLong = startIpInLong;
        this.endIpInLong = endIpInLong;
        this.province = province;
    }

    public long getStartIpInLong() {
        return startIpInLong;
    }

    public long getEndIpInLong() {
        return endIpInLong;
    }

    public String getProvince() {
        return province;
    }
}
