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

import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.sdmxsource.sdmx.api.model.beans.codelist.CodeBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;

import de.gerdiproject.harvest.etls.AbstractETL;
import de.gerdiproject.harvest.etls.EurostatETL;
import de.gerdiproject.harvest.etls.extractors.SdmxVO;
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
public class EurostatTransformer extends AbstractIteratorTransformer<SdmxVO, DataCiteJson>
{
    private EurostatETL eurostatETL;

    @Override
    public void init(AbstractETL<?, ?> etl)
    {
        super.init(etl);
        eurostatETL = (EurostatETL) etl;
    }

    /**
     * Returns a DataCiteJson for a given dimension selection.
     *
     * Will be called multiple times for one SdmxVO
     *
     * @param source SdmxVO (for global information)
     * @param dimensionSelection the specific selection of dimension + values
     *
     * @return The DataCiteJSON document
     */
    @Override
    protected DataCiteJson transformElement(SdmxVO source)
    {
        String identifier = getIdentifier(source);
        DataCiteJson document = createDataCiteStub(identifier);

        document.addTitles(getTitle(source));
        document.setPublisher(eurostatETL.getPublisher());
        document.addSubjects(getSubjects(source));
        document.setLanguage(eurostatETL.getLanguage());
        document.addFormats(eurostatETL.getFormats());
        document.addRights(eurostatETL.getRightsList());
        document.addDescriptions(getDescription(source));

        if (hasGeoDimension(source))
            document.addGeoLocations(getGeoLocations(source));

        document.addResearchData(getResearchData(source));
        return document;
    }

    /**
     * Returns an Identifier for the document.
     *
     * The identifier will not be a DOI, but a URL to the REST-URL
     * which can be used to retrieve exactle the data corresponding to the
     * selection of dimension + value.
     *
     * @param source value object
     *
     * @return String
     */
    private String getIdentifier(SdmxVO source)
    {
        StringBuilder queryBuilder = new StringBuilder();

        for (Map.Entry<String, CodeBean> entry : source.getDimensions().entrySet()) {
            if (queryBuilder.length() != 0)
                queryBuilder.append(EurostatConstants.QUERY_PARAM_SEPARATOR);

            queryBuilder.append(
                String.format(EurostatConstants.QUERY_PARAM_FORMAT,
                              entry.getKey(),
                              entry.getValue().getId()));
        }

        String queryString = queryBuilder.toString();

        return String.format(
                   EurostatConstants.IDENTIFIER_FORMAT,
                   eurostatETL.getRestBaseUrl(),
                   source.getDataStructureBean().getId().replaceFirst("DSD_", ""),
                   queryString);
    }

    /**
     * Creates a title for the document.
     *
     * The title is composed of the name of the SDMX Dataflow and the
     * selection of dimension + value.
     *
     * @param source value object
     *
     * @return Collection with one title
     */
    private Collection<Title> getTitle(SdmxVO source)
    {
        StringBuilder stringBuilder = new StringBuilder();

        for (Map.Entry<String, CodeBean> entry : source.getDimensions().entrySet()) {
            if (stringBuilder.length() != 0)
                stringBuilder.append(EurostatConstants.TITLE_DIMENSION_SEPARATOR);

            final String dimension = String.format(
                                         EurostatConstants.TITLE_DIMENSION_FORMAT,
                                         entry.getValue().getName());

            stringBuilder.append(dimension);
        }

        final String titleString = String.format(
                                       EurostatConstants.TITLE_FORMAT,
                                       source.getEnglishOrFirstName(),
                                       stringBuilder.toString());

        return Arrays.asList(new Title(titleString));
    }

    /**
     * Creates a collection of subjects for the document.
     *
     * The subjects correspond to the name of the dimensions of the dimensionSelection.
     *
     * @param source value object
     *
     * @return Collection with subjects
     */
    private Collection<Subject> getSubjects(SdmxVO source)
    {
        final List<Subject> subjects = new LinkedList<>();

        for (Map.Entry<String, CodeBean> entry : source.getDimensions().entrySet()) {
            Subject subject = new Subject(entry.getValue().getName(),
                                          EurostatConstants.LANGUAGE_DEFAULT_VALUE);
            subjects.add(subject);
        }

        return subjects;
    }

    /**
     * Creates a description for the document.
     *
     * The description is composed of the name of the SDMX Dataflow.
     *
     * @param source value object
     *
     * @return Collection with one description
     */
    private Collection<Description> getDescription(SdmxVO source)
    {
        StringBuilder stringBuilder = new StringBuilder();

        for (Map.Entry<String, CodeBean> entry : source.getDimensions().entrySet()) {
            if (stringBuilder.length() != 0)
                stringBuilder.append(EurostatConstants.DESCRIPTION_DIMENSION_SEPARATOR);

            final String dimension = String.format(
                                         EurostatConstants.DESCRIPTION_DIMENSION_FORMAT,
                                         entry.getKey(),
                                         entry.getValue().getId(),
                                         entry.getValue().getName());

            stringBuilder.append(dimension);
        }

        final String descriptionString = String.format(
                                             EurostatConstants.DESCRIPTION_FORMAT,
                                             source.getEnglishOrFirstName(),
                                             stringBuilder.toString());

        return Arrays.asList(new Description(
                                 descriptionString,
                                 DescriptionType.Abstract));
    }

    /**
     * Indicator whether there is geo-related information in the dimensions.
     *
     *
     * @param source value object
     *
     * @return Boolean indicating the availability of geo-related information
     */
    private boolean hasGeoDimension(SdmxVO source)
    {
        if (source.getDimensions().get(EurostatConstants.GEO_DIMENSION) != null)
            return true;

        return false;
    }

    /**
     * Creates a geoLocation-field for the document.
     *
     * We only use the name to set geoLocationName
     *
     * @param source value object
     *
     * @return Collection with one GeoLocation
     */
    private Collection<GeoLocation> getGeoLocations(SdmxVO source)
    {
        final List<GeoLocation> geoLocations = new LinkedList<>();
        geoLocations.add(
            new GeoLocation(
                source.getDimensions().get(
                    EurostatConstants.GEO_DIMENSION).getName()));
        return geoLocations;
    }

    /**
     * Creates ResearchData information for the document.
     *
     * The link to the research data is identical to the identifier of the document
     *
     * @param source value object
     *
     * @return Collection with one ResearchData object
     */
    private Collection<ResearchData> getResearchData(SdmxVO source)
    {
        List<ResearchData> researchData = new LinkedList<>();
        researchData.add(new ResearchData(
                             getIdentifier(source),
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
