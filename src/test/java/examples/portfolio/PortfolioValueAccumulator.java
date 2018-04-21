package examples.portfolio;

import net.openhft.chronicle.core.values.LongValue;
import net.openhft.chronicle.map.MapEntry;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.util.function.Consumer;

public final class PortfolioValueAccumulator implements Consumer<MapEntry<LongValue, PortfolioAssetInterface>> {
    final MutableDouble total;
    final PortfolioAssetInterface asset;

    public PortfolioValueAccumulator(MutableDouble total, PortfolioAssetInterface asset) {
        this.total = total;
        this.asset = asset;
    }

    @Override
    public void accept(MapEntry<LongValue, PortfolioAssetInterface> e) {
        e.value().getUsing(asset);
        total.add(asset.getShares() * asset.getPrice());
    }
}