/*
 * Copyright 2013-2025 chronicle.software; SPDX-License-Identifier: Apache-2.0
 */
package examples.portfolio;

public interface PortfolioAssetInterface {
    long getAssetId();

    void setAssetId(long assetId);

    int getShares();

    void setShares(int shares);

    double getPrice();

    void setPrice(double price);
}
