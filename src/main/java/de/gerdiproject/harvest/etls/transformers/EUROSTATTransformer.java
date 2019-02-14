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
package de.gerdiproject.harvest.etls.transformers;

import java.util.Calendar;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.LinkedList;

import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.codelist.CodeBean;

import de.gerdiproject.harvest.etls.AbstractETL;
import de.gerdiproject.harvest.etls.EUROSTATETL;
import de.gerdiproject.harvest.etls.sdmx.DataStructure;
import de.gerdiproject.json.datacite.DataCiteJson;
import de.gerdiproject.json.datacite.Description;
import de.gerdiproject.json.datacite.GeoLocation;
import de.gerdiproject.json.datacite.Identifier;
import de.gerdiproject.json.datacite.Subject;
import de.gerdiproject.json.datacite.Title;
import de.gerdiproject.json.datacite.extension.generic.ResearchData;

/**
 * This transformer parses metadata from a {@linkplain DataStructureBean}
 * and creates {@linkplain LinkedList<DataCiteJson>} objects from it.
 *
 * @author Tobias Weber
 */
public class EUROSTATTransformer extends AbstractIteratorTransformer<DataStructure, LinkedList<DataCiteJson>>
{
    private EUROSTATETL eurostatETL;

    @Override
    public void init(AbstractETL<?, ?> etl)
    {
        super.init(etl);
        eurostatETL = (EUROSTATETL) etl;
    }

    @Override
    protected LinkedList<DataCiteJson> transformElement(DataStructure source)
    {
        LinkedList<DataCiteJson> records = new LinkedList<DataCiteJson>();

        LinkedList<HashMap<String, CodeBean>> dimensionCombinations
            = getDimensionCombinations(source);

        for (HashMap<String, CodeBean> dimensionCombination : dimenstionCombinations)
            records.add(getDocument(source, dimensionCombination));

        return records;
    }

    /**
     * Get a list of dimension values which are both configured and present in the data structure
     *
     * @param the DataStructure in question
     *
     * @return a list of all possible combinations of codes of each allowed and present dimension
     */
    private List<Map<String, CodeBean>> getDimensionCombinations(DataStructure source)
    {
        List<String> dimensionIds = new LinkedList<String>();

        // get all dimensions that are allowed AND existent in source
        for (DimensionBean dimensionBean : source.getDataStructureBean.getDimensions()) {
            if (getAllowedDimensionNames().contains(dimensionBean.getId()))
                dimensionId.add(dimensionBean.getId());
        }

        //Convert dimensions to a List.
        HashMap<String, List<CodeBean>> input = new HashMap<String, List<CodeBean>>();

        for (String dimensionId : dimensionIds)
            input.put(dimensionId, getCodeList(source, dimensionId));

        //Build a list of all possible combination of values of each dimension.
        return combineDimensions(0,
                                 input,
                                 new HashMap<String, CodeBean>(),
                                 new LinkedList <Map<String, CodeBean>>()
                                );
    }

    /**
     * Get a list of all Codes given a dimensionId
     *
     * @param source the DataStructure to be searched
     * @param dimensionId the ID of the dimension to be iterated over
     *
     * @return A list of all code values for the dimension in source
     */
    private List<CodeBean> getCodeList(DataStructure source, String dimensionId)
    {
        LinkedList<String> codes = new LinkedList<String>();

        DimensionBean dimensionBean = source.getDataStructureBean().getDimension(dimensionId);
        ConceptBean conceptBean = (ConcepBean) dimensionBean.getConceptRole().getTargetReference();
        CodeListBean codeListBean
            = (CodeListBean) conceptBean.getCoreRepresentation().getRepresentation().getTargetReference();

        for (SDMXBean code : codeListBean)
            codes.add((CodeBean) code);
    }

    /**
     * This function transforms a map of n sets of Strings with arbitrary lengths
     * to a set of maps with n key/value pairs, comprising all possible combinations
     * of the elements of the input maps.
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
     * We use recursion to traverse to each leaf of this tree
     *
     *                  |
     *         ------------------
     *        "a"              "the"
     *         |                 |
     *       "fat"             "fat"
     *         |                 |
     *   -------------     -------------
     *   |     |     |     |     |     |
     * "cop" "god" "cod" "cop" "god" "cod"
     *
     * Idea for this implementation:
     * https://stackoverflow.com/questions/8173862/map-of-sets-into-list-of-all-combinations
     *
     * @param level tracks the current level in the tree or number of recursions happened.
     * @param input Map with the Sets to be combined
     * @param currentRow the current combination that is assembled
     * @param output intermediate list of combinations to be calculated (manage state among recursions)
     *
     * @return list of combinations of the element of the input maps
     */

    private static List<Map <String, CodeBean>> combineDimensions(
        int level,
        Map<String, List <CodeBean>> input,
        Map<String, CodeBean> currentRow,
        List<Map <String, CodeBean>> output)
    {
        if (level == input.size()) { //all dimensions were visited -> we have reached a leaf
            // deep copy of current, because Java sucks at doing this natively
            Map<String, CodeBean> newMap = new HashMap<String, CodeBean>();

            for (String key : currentRow.keySet())
                newMap.put(key, currentRow.get(key));

            output.add(newMap);
            return output;
        } else { //we have not visited a leaf, so we need to iterate over all values of this level
            String dimension = (String) input.keySet().toArray()[level];

            for (CodeBean codeBean : input.get(dimension)) {
                currentRow.put(dimension, codeBean);
                output = combineDimensions(level + 1, input, currentRow, output);
                // After a leaf was visited we remove the key
                // (otherwise the current key/value-pair would be part of all rows added to the output).
                currentRow.remove(dimension);
            }
        }

        return output;
    }


    /**
     * A list of dimensions that are supported
     *
     * @return list of names of dimensions as strings
     */
    private List<String> getAllowedDimensionNames()
    {
        return new ArrayList<String>() {
            {
                add("NA_ITEM");
                add("GEO");
                add("UNIT");
            }
        };
    }

    private DataCiteJson getDocument(DataStructure source,
                                     Map<String, CodeBean> dimensionSelection)
    {
        Identifier identifier = new Identifier(getIdentifier(source, dimensionSelection));
        DataCiteJson document = createDataCiteStub(identifier);

        document.addTitles(getTitle(source, dimenstionSelection));
        document.setPublisher(etl.getPublisher());
        document.addSubjects(getSubjects(dimensionSelection));
        document.setLanguage(etl.getLanguage());
        document.addFormats(etl.getFormats());
        document.addRights(etl.getRightsList());
        document.addDescriptions(getDescription(source, dimensionSelection));

        if (hasGeoDimension(source))
            documentaddGeoLocations(getGeoLocations(source));

        document.addResearchData(getResearchData(source, dimensionSelection));
    }

    private String getIdentifier(DataStructure source,
                                 Map<String, CodeBean> dimensionSelection)
    {
        StringBuilder identifierBuilder = new StringBuilder();
        identifierBuilder.append(etl.getRestBaseUrl());
        identifierBuilder.append("/");
        identifierBuilder.append(source.getDataStructureBean().getId());
        identifierBuilder.append("?");

        for (String key : dimensionSelection.keySet()) {
            identifierBuilder.append(key);
            identifierBuilder.append("=");
            identifierBuilder.append(dimensionSelection.get(key).getName());
            identifierBuilder.append("&");
        }

        //delete the last "&"
        identifierBuilder.setLenght(identifierBuilder.length() - 1);
        return (identifierBuilder.toString());
    }

    /**
     * Creates a title from the name of the DataStructure and all dimensionCodes
     *
     * @param source DataStructure
     * @param dimensionSelection hash (dimensionName -> codeValue)
     *
     * @return Collection with one title
     */
    private Collection<Title> getTitle(DataStructure source,
                                       Map<String, CodeBean> dimensionSelection)
    {
        ArrayList titles = new ArrayList();
        StringBuilder titleBuilder = StringBuilder();
        titleBuilder.append(source.getEnglishOrFirstName());
        titleBuilder.append("(");

        for (String key : dimensionSelection.keySet()) {
            titleBuilder.append(key);
            titleBuilder.append(": ");
            titleBuilder.append(dimensionSelection.get(key).getName());
        }

        titleBuilder.append(")");
        titles.add(titleBuilder.toString());
        return titles;
    }

    private Collection<Subject> getSubjects(Map<String, CodeBean> dimensionSelection)
    {
        ArrayList subjects = new ArrayList();

        for (String key : dimensionSelection.keySet()) {
            Subject subject = new Subject(key, EUROSTATConstants.LANGUAGE_DEFAULT_VALUE);
            subjects.add(subject);
        }

        return subjects;

    }

    private Collection<Description> getDescription(DataStructure source,
                                                   Map<String, CodeBean> dimensionSelection)
    {
        StringBuilder descriptionBuilder = new StringBuilder();
        descriptionBuilder.append(source.getEnglishOrFirstName());
    }

    private boolean hasGeoDimension(Map<String, CodeBean> dimensionSelection)
    {
        if (dimensionSelection.get("GEO") != null)
            return true;

        return false;
    }

    private Collection<GeoLocation> getGeoLocations(
        Map<String, CodeBean> dimensionSelection)
    {
        ArrayList geoLocations = new ArrayList();
        geoLocations.add(new GeoLocation(dimensionSelection.get("GEO").getName()));
        return geoLocations;
    }

    private Collection<ResearchData> getResearchData(DataStructure source,
                                                     Map<String, CodeBean> dimensionSelection)
    {
        ArrayList researchData = new ArrayList();
        researchData.add(new ResearchData(
                             getIdentifier(source, dimensionSelection),
                             source.getEnglishOrFirstName()));
        return researchData;
    }

    /**
     * Creates a stub for a DataCiteJson object (with fields that are
     * identical for all documents
     *
     * @param Identifier identifier for the DataCiteJson
     *
     * @return the DataCiteJson document
     */
    private DataCiteJson createDataCiteStub(Identifier identifier)
    {
        DataCiteJson document = new DataCiteJson(identifier);

        document.setPublisher(etl.getPublisher());
        document.setPublicationYear(
            String.valueOf(Calender.getInstance().get(Calender.Year)));
        document.setLanguage(etl.getLanguage());
        document.setResourceType(
            new ResourceType("Statistical Data", ResourceTypeGeneral.Dataset));
        document.setFormats(etl.getFormats());
        document.setRightsList(etl.getRightsList());

        return document;
    }
}
