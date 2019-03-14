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
import java.util.LinkedList;
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

        // calculate the number of list entries in the output list
        int outputListSize = 1;

        for (Map.Entry<K, List<V>> e : inputEntries)
            outputListSize *= e.getValue().size();

        // initialize all maps
        final List<Map<K, V>> outputList = new ArrayList<>(outputListSize);

        for (int i = 0; i < outputListSize; i++)
            outputList.add(new HashMap<>());

        // fill data
        int numberOfSeenCombinations = 1;

        for (Map.Entry<K, List<V>> e : inputEntries) {
            final K key = e.getKey();
            final List<V> values = e.getValue();
            final int numberOfValueOccurrences = outputListSize / values.size();

            for (int valueIndex = 0; valueIndex < values.size(); valueIndex++) {
                final V val = values.get(valueIndex);

                for (int repeated = 0; repeated < numberOfValueOccurrences; repeated++) {
                    int insertAt = getPosition(repeated, valueIndex, numberOfSeenCombinations, values.size());
                    outputList.get(insertAt).put(key, val);
                }
            }

            numberOfSeenCombinations *= values.size();
        }

        return outputList;
    }

    /**
     * Gets the position in a list in which the repeatedth occurrence of value
     * with valueIndex has to be inserted, given that we already iterated over
     * numberOfSeenCombinations and that the current dimension has
     * sizeOfCurrentDimension elements.
     *
     * @param repeated how often the value has been inserted yet
     * @param valueIndex the position of the value in the current dimension
     * @param numberOfSeenCombinations the number of combinations already seen
     * @param sizeOfCurrentDimension the size of the current dimension
     *
     * @return position in the list in which the repeatedth occurrence has to be inserted
     */
    public static int getPosition(int repeated, int valueIndex, int numberOfSeenCombinations, int sizeOfCurrentDimension)
    {
        int remainder = repeated % numberOfSeenCombinations;
        int quotient = repeated / numberOfSeenCombinations;
        return valueIndex * numberOfSeenCombinations + quotient * numberOfSeenCombinations * sizeOfCurrentDimension + remainder;
    }
}
