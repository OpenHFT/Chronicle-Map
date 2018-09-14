/*
 * Copyright 2012-2018 Chronicle Map Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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