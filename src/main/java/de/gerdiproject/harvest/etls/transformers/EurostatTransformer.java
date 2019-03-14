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
        StringBuilder identifierBuilder = new StringBuilder();
        identifierBuilder.append(eurostatETL.getRestBaseUrl())
        .append('/')
        .append(source.getDataStructureBean().getId())
        .append('?');

        for (Map.Entry<String, CodeBean> entry : source.getDimensions().entrySet()) {
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
     * @param source value object
     *
     * @return Collection with one title
     */
    private Collection<Title> getTitle(SdmxVO source)
    {
        final List<Title> titles = new LinkedList<>();
        StringBuilder titleBuilder = new StringBuilder();
        titleBuilder.append(source.getEnglishOrFirstName())
        .append('(');

        for (Map.Entry<String, CodeBean> entry : source.getDimensions().entrySet()) {
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
     * @param source value object
     *
     * @return Collection with subjects
     */
    private Collection<Subject> getSubjects(SdmxVO source)
    {
        final List<Subject> subjects = new LinkedList<>();

        for (String key : source.getDimensions().keySet()) {
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
     * @param source value object
     *
     * @return Collection with one description
     */
    private Collection<Description> getDescription(SdmxVO source)
    {
        final List<Description> descriptions = new LinkedList<>();

        StringBuilder descriptionBuilder = new StringBuilder();
        descriptionBuilder.append(source.getEnglishOrFirstName())
        .append('\n');

        for (Map.Entry<String, CodeBean> entry : source.getDimensions().entrySet()) {
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
