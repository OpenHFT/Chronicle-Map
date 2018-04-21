package examples.portfolio;

public interface PortfolioAssetInterface {
    public long getAssetId();

    public void setAssetId(long assetId);

    public int getShares();

    public void setShares(int shares);

    public double getPrice();

    public void setPrice(double price);
}