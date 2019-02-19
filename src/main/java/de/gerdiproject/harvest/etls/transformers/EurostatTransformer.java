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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.sdmxsource.sdmx.api.model.beans.base.SDMXBean;
import org.sdmxsource.sdmx.api.model.beans.codelist.CodeBean;
import org.sdmxsource.sdmx.api.model.beans.codelist.CodelistBean;
import org.sdmxsource.sdmx.api.model.beans.conceptscheme.ConceptBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.beans.reference.CrossReferenceBean;

import de.gerdiproject.harvest.etls.AbstractETL;
import de.gerdiproject.harvest.etls.EurostatETL;
import de.gerdiproject.harvest.etls.SDMXDataChunk;
import de.gerdiproject.harvest.eurostat.constants.EurostatConstants;
import de.gerdiproject.json.datacite.DataCiteJson;
import de.gerdiproject.json.datacite.Description;
import de.gerdiproject.json.datacite.GeoLocation;
import de.gerdiproject.json.datacite.ResourceType;
import de.gerdiproject.json.datacite.Subject;
import de.gerdiproject.json.datacite.Title;
import de.gerdiproject.json.datacite.enums.DescriptionType;
import de.gerdiproject.json.datacite.enums.ResourceTypeGeneral;
import de.gerdiproject.json.datacite.extension.generic.ResearchData;

/**
 * This transformer parses metadata from a {@linkplain DataStructureBean}
 * and creates {@linkplain LinkedList<DataCiteJson>} objects from it.
 *
 * @author Tobias Weber
 */
public class EurostatTransformer extends AbstractIteratorTransformer<SDMXDataChunk, LinkedList<DataCiteJson>>
{
    private EurostatETL eurostatETL;

    @Override
    public void init(AbstractETL<?, ?> etl)
    {
        super.init(etl);
        eurostatETL = (EurostatETL) etl;
    }

    @Override
    protected LinkedList<DataCiteJson> transformElement(SDMXDataChunk source)
    {
        LinkedList<DataCiteJson> records = new LinkedList<DataCiteJson>();

        List<Map<String, CodeBean>> dimensionCombinations
            = getDimensionCombinations(source);

        for (Map<String, CodeBean> dimensionCombination : dimensionCombinations)
            records.add(getDocument(source, dimensionCombination));

        return records;
    }

    /**
     * Get a list of dimension values which are both configured and present in the data structure
     *
     * @param the SDMXDataChunk in question
     *
     * @return a list of all possible combinations of codes of each allowed and present dimension
     */
    private List<Map<String, CodeBean>> getDimensionCombinations(SDMXDataChunk source)
    {
        List<String> dimensionIds = new LinkedList<String>();

        // get all dimensions that are allowed AND existent in source
        for (DimensionBean dimensionBean : source.getDataStructureBean().getDimensions()) {
            if (getAllowedDimensionNames().contains(dimensionBean.getId()))
                dimensionIds.add(dimensionBean.getId());
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
     * @param source the SDMXDataChunk to be searched
     * @param dimensionId the ID of the dimension to be iterated over
     *
     * @return A list of all code values for the dimension in source
     */
    private List<CodeBean> getCodeList(SDMXDataChunk source, String dimensionId)
    {
        LinkedList<CodeBean> codes = new LinkedList<CodeBean>();

        DimensionBean dimensionBean = source.getDataStructureBean().getDimension(dimensionId);
        List<CrossReferenceBean> conceptRole = dimensionBean.getConceptRole();
        ConceptBean conceptBean
            = (ConceptBean) conceptRole.get(0).createMutableInstance().getMaintainableReference();

        CodelistBean codeListBean
            = (CodelistBean) conceptBean.getCoreRepresentation()
              .getRepresentation()
              .createMutableInstance()
              .getMaintainableReference();

        for (SDMXBean code : codeListBean.getItems())
            codes.add((CodeBean) code);

        return codes;
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

            for (Map.Entry<String, CodeBean> entry : currentRow.entrySet())
                newMap.put(entry.getKey(), entry.getValue());

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

    /**
     * Returns a DataCiteJson for a given dimension selection.
     *
     * Will be called multiple times for one SDMXDataChunk.
     *
     * @param source SDMXDataChunk (for global information)
     * @param dimensionSelection the specific selection of dimension + values
     *
     * @return The DataCiteJSON document
     */
    private DataCiteJson getDocument(SDMXDataChunk source,
                                     Map<String, CodeBean> dimensionSelection)
    {
        String identifier = getIdentifier(source, dimensionSelection);
        DataCiteJson document = createDataCiteStub(identifier);

        document.addTitles(getTitle(source, dimensionSelection));
        document.setPublisher(eurostatETL.getPublisher());
        document.addSubjects(getSubjects(dimensionSelection));
        document.setLanguage(eurostatETL.getLanguage());
        document.addFormats(eurostatETL.getFormats());
        document.addRights(eurostatETL.getRightsList());
        document.addDescriptions(getDescription(source, dimensionSelection));

        if (hasGeoDimension(dimensionSelection))
            document.addGeoLocations(getGeoLocations(dimensionSelection));

        document.addResearchData(getResearchData(source, dimensionSelection));
        return document;
    }

    /**
     * Returns an Identifier for the document.
     *
     * The identifier will not be a DOI, but a URL to the REST-URL
     * which can be used to retrieve exactle the data corresponding to the
     * selection of dimension + value.
     *
     * @param source SDMXDataChunk (for global information)
     * @param dimensionSelection the specific selection of dimension + values
     *
     * @return String
     */
    private String getIdentifier(SDMXDataChunk source,
                                 Map<String, CodeBean> dimensionSelection)
    {
        StringBuilder identifierBuilder = new StringBuilder();
        identifierBuilder.append(eurostatETL.getRestBaseUrl())
        .append('/')
        .append(source.getDataStructureBean().getId())
        .append('?');

        for (Map.Entry<String, CodeBean> entry : dimensionSelection.entrySet()) {
            identifierBuilder.append(entry.getKey())
            .append('=')
            .append(entry.getValue().getName())
            .append('&');
        }

        //delete the last "&"
        identifierBuilder.setLength(identifierBuilder.length() - 1);
        return identifierBuilder.toString();
    }

    /**
     * Creates a title for the document.
     *
     * The title is composed of the name of the SDMX Dataflow and the
     * selection of dimension + value.
     *
     * @param source SDMXDataChunk
     * @param dimensionSelection the specific selection of dimension + values
     *
     * @return Collection with one title
     */
    private Collection<Title> getTitle(SDMXDataChunk source,
                                       Map<String, CodeBean> dimensionSelection)
    {
        final List<Title> titles = new LinkedList<>();
        StringBuilder titleBuilder = new StringBuilder();
        titleBuilder.append(source.getEnglishOrFirstName())
        .append('(');

        for (Map.Entry<String, CodeBean> entry : dimensionSelection.entrySet()) {
            titleBuilder.append(entry.getKey())
            .append(": ")
            .append(entry.getValue().getName())
            .append(", ");
        }

        //delete the last ", "
        titleBuilder.setLength(titleBuilder.length() - 2);
        titleBuilder.append(')');
        titles.add(new Title(titleBuilder.toString()));
        return titles;
    }

    /**
     * Creates a collection of subjects for the document.
     *
     * The subjects correspond to the name of the dimensions of the dimensionSelection.
     *
     * @param dimensionSelection the specific selection of dimension + values
     *
     * @return Collection with subjects
     */
    private Collection<Subject> getSubjects(Map<String, CodeBean> dimensionSelection)
    {
        final List<Subject> subjects = new LinkedList<>();

        for (String key : dimensionSelection.keySet()) {
            Subject subject = new Subject(key, EurostatConstants.LANGUAGE_DEFAULT_VALUE);
            subjects.add(subject);
        }

        return subjects;

    }

    /**
     * Creates a description for the document.
     *
     * The description is composed of the name of the SDMX Dataflow.
     *
     * @param source SDMXDataChunk
     *
     * @return Collection with one description
     */
    private Collection<Description> getDescription(SDMXDataChunk source,
                                                   Map<String, CodeBean> dimensionSelection)
    {
        final List<Description> descriptions = new LinkedList<>();

        StringBuilder descriptionBuilder = new StringBuilder();
        descriptionBuilder.append(source.getEnglishOrFirstName());
        descriptionBuilder.append("\n");

        for (Map.Entry<String, CodeBean> entry : dimensionSelection.entrySet()) {
            descriptionBuilder.append(entry.getKey())
            .append(": ")
            .append(entry.getValue().getName())
            .append(", ");
        }

        descriptions.add(new Description(
                             descriptionBuilder.toString(),
                             DescriptionType.Abstract));

        return descriptions;
    }

    /**
     * Indicator whether there is geo-related information in the dimensions.
     *
     *
     * @param dimensionSelection the specific selection of dimension + values
     *
     * @return Boolean indicating the availability of geo-related information
     */
    private boolean hasGeoDimension(Map<String, CodeBean> dimensionSelection)
    {
        if (dimensionSelection.get(EurostatConstants.GEO_DIMENSION) != null)
            return true;

        return false;
    }

    /**
     * Creates a geoLocation-field for the document.
     *
     * We only use the name to set geoLocationName
     *
     * @param dimensionSelection the specific selection of dimension + values
     *
     * @return Collection with one GeoLocation
     */
    private Collection<GeoLocation> getGeoLocations(
        Map<String, CodeBean> dimensionSelection)
    {
        final List<GeoLocation> geoLocations = new LinkedList<>();
        geoLocations.add(
            new GeoLocation(
                dimensionSelection.get(
                    EurostatConstants.GEO_DIMENSION).getName()));
        return geoLocations;
    }

    /**
     * Creates ResearchData information for the document.
     *
     * The link to the research data is identical to the identifier of the document
     *
     * @param source SDMXDataChunk
     * @param dimensionSelection the specific selection of dimension + values
     *
     * @return Collection with one ResearchData object
     */
    private Collection<ResearchData> getResearchData(SDMXDataChunk source,
                                                     Map<String, CodeBean> dimensionSelection)
    {
        List<ResearchData> researchData = new LinkedList<>();
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
    private DataCiteJson createDataCiteStub(String identifier)
    {
        DataCiteJson document = new DataCiteJson(identifier);

        document.setPublisher(eurostatETL.getPublisher());
        document.setPublicationYear(Calendar.getInstance().get(Calendar.YEAR));
        document.setLanguage(eurostatETL.getLanguage());
        document.setResourceType(
            new ResourceType("Statistical Data", ResourceTypeGeneral.Dataset));
        document.addFormats(eurostatETL.getFormats());
        document.addRights(eurostatETL.getRightsList());

        return document;
    }
}
