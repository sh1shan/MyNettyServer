package com.dragon.netty.utils;


public enum ResultCode {

    SUCCESS("0000", "SUCCESS", "success"),
    STORM_PASS("1000", "STORM_PASS", "storm决策通过"),
    STORM_REFUSE("2000", "STORM_REFUSE", "storm决策拒绝"),
    STORM_TIMEOUT("3001", "STORM_TIMEOUT", "storm决策超时"),
    STORM_ERROR("3002", "STORM_ERROR", "storm决策没超时，但决策没结果"),
    CLIENT_ERROR("3003", "CLIENT_ERROR", "获取httpSenderClient失败");


    private String code;
    private String desc;
    private String detail;

    ResultCode(String code, String desc, String detail) {
        this.code = code;
        this.desc = desc;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }
}
