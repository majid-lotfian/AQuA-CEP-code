package FlinkCEP01;


import java.io.Serializable;

public class AttributeValue implements Serializable {
    private String AttributeName;
    private String AttributeValue;

    public AttributeValue(String name, String value){
        this.AttributeName=name;
        this.AttributeValue=value;
    }
    public AttributeValue(){}

    public boolean isBiggerThan(String s){
        boolean result = false;
        double value = Double.parseDouble(s);
        if (Double.parseDouble(this.AttributeValue) > value){
            result = true;
        }
        return result;
    }

    public boolean isSmallerThan(String s){
        boolean result = false;
        double value = Double.parseDouble(s);
        if (Double.parseDouble(this.AttributeValue) < value){
            result = true;
        }
        return result;
    }

    public boolean equal(String s){
        boolean result = false;
        double value = Double.parseDouble(s);
        if (Double.parseDouble(this.AttributeValue) == value){
            result = true;
        }
        return result;
    }

    public boolean notEqual(String s){
        boolean result = false;
        double value = Double.parseDouble(s);
        if (Double.parseDouble(this.AttributeValue) != value){
            result = true;
        }
        return result;
    }

    public String getAttributeName() {
        return AttributeName;
    }

    public String getAttributeValue() {
        return AttributeValue;
    }

    public void setAttributeValue(String attributeValue) {
        AttributeValue = attributeValue;
    }

    public void setAttributeName(String attributeName) {
        AttributeName = attributeName;
    }
}

