package Util;
import java.io.Serializable;


public class InfoUnit implements Serializable {
    private static final long serialVersionUID = 1L;
    private String IDCard;
    private String marketID;
    private String time;

    public String getIDCard() {
        return IDCard;
    }

    public void setIDCard(String IDCard) {
        this.IDCard = IDCard;
    }

    public String getMarketID() {
        return marketID;
    }

    public void setMarketID(String marketID) {
        this.marketID = marketID;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
