/*
 *      Copyright (C) 2012, 2016  higherfrequencytrading.com
 *      Copyright (C) 2016 Roman Leventov
 *
 *      This program is free software: you can redistribute it and/or modify
 *      it under the terms of the GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License.
 *
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU Lesser General Public License for more details.
 *
 *      You should have received a copy of the GNU Lesser General Public License
 *      along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package net.openhft.chronicle.hash.impl.stage.query;

import net.openhft.chronicle.hash.impl.stage.entry.Alloc;
import net.openhft.chronicle.hash.impl.stage.entry.SegmentStages;
import net.openhft.sg.StageRef;
import net.openhft.sg.Staged;

@Staged
public class QueryAlloc implements Alloc {

    @StageRef
    public SegmentStages s;

    @Override
    public long alloc(int chunks, long prevPos, int prevChunks) {
        long ret = s.allocReturnCode(chunks);
        if (prevPos >= 0)
            s.free(prevPos, prevChunks);
        if (ret >= 0)
            return ret;
        int alreadyAttemptedTier = s.tier;
        s.goToFirstTier();
        while (true) {
            if (s.tier != alreadyAttemptedTier) {
                ret = s.allocReturnCode(chunks);
                if (ret >= 0)
                    return ret;
            }
            s.nextTier();
        }
    }
}
