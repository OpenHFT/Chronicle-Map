/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package examples.portfolio;

public interface PortfolioAssetInterface {
    public long getAssetId();

    public void setAssetId(long assetId);

    public int getShares();

    public void setShares(int shares);

    public double getPrice();

    public void setPrice(double price);
}
