/**
 * Copyright © 2019 Tobias Weber (http://www.gerdi-project.de)
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
package de.gerdiproject.harvest.etls;

import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class bundles static helper methods for SDMX processing
 *
 * @author Tobias Weber
 */
public class SdmxUtil
{
    /**
     * Transforms a map of n lists to a list of maps with n key/value pairs,
     * comprising all possible combinations of the elements of the input maps.
     *
     * Example:
     * Input: { "article":  ["a", "the"],
     *          "adjective: ["fat"],
     *          "noun": ["cop", "god", "cod"] }
     * Output: [
     *          { "article" : "a",   "adjective" : "fat", "noun" : "cop" },
     *          { "article" : "a",   "adjective" : "fat", "noun" : "god" },
     *          { "article" : "a",   "adjective" : "fat", "noun" : "cod" },
     *          { "article" : "the", "adjective" : "fat", "noun" : "cop" },
     *          { "article" : "the", "adjective" : "fat", "noun" : "god" },
     *          { "article" : "the", "adjective" : "fat", "noun" : "cod" },
     *
     *
     * @param input map with the lists to be transformed
     *
     * @return list of maps with all combinations
     */
    public static <K, V> List<Map<K, V>> mapOfListsToListOfMaps(Map<K, List <V>> input)
    {
        final Set<Map.Entry<K, List<V>>> inputEntries = input.entrySet();

        // calculate number of rows
        int rowCount = 1;

        for (Map.Entry<K, List<V>> e : inputEntries)
            rowCount *= e.getValue().size();

        // initialize all maps
        final List<Map<K, V>> output = new ArrayList<>(rowCount);

        for (int i = 0; i < rowCount; i++)
            output.add(new HashMap<>());

        // fill data
        int repetitionsPerChunk = rowCount;

        for (Map.Entry<K, List<V>> e : inputEntries) {
            final K key = e.getKey();
            final List<V> values = e.getValue();

            //at the beginning of each iteration repetitionsPerChunk points to the number of rows
            //that were processed en bloc in the last iteration. 
            //This will serve as the information how many rows we need to skip after a chunk has been processed.
            final int skip = repetitionsPerChunk;

            repetitionsPerChunk /= values.size();

            for (int v = 0; v < values.size(); v++) {
                final V val = values.get(v);

                for (int offset = v * repetitionsPerChunk; offset < rowCount; offset += skip)
                    for (int r = 0; r < repetitionsPerChunk; r++)
                        output.get(offset + r).put(key, val);
            }
        }

        return output;
    }
}
