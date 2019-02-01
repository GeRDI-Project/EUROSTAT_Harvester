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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.function.Function;

import org.sdmxsource.sdmx.api.model.beans.reference.CrossReferenceBean;

import de.gerdiproject.harvest.config.Configuration;
import de.gerdiproject.harvest.config.parameters.StringParameter;
import de.gerdiproject.harvest.config.parameters.constants.ParameterMappingFunctions;
import de.gerdiproject.harvest.etls.constants.EUROSTATConstants;
import de.gerdiproject.harvest.etls.extractors.EUROSTATExtractor;
import de.gerdiproject.harvest.etls.transformers.EUROSTATTransformer;
import de.gerdiproject.json.datacite.DataCiteJson;
import de.gerdiproject.json.datacite.Rights;
import de.gerdiproject.json.datacite.Formats;

/**
 * An ETL for harvesting EUROSTAT.<br>
 * See: https://ec.europa.eu/eurostat
 *
 * @author Tobias Weber
 */
public class EUROSTATETL extends StaticIteratorETL<CrossReferenceBean, DataCiteJson>
{
    // URLs
    private StringParameter sdemUrlParam;
    private StringParameter logoUrlParam;
    // configurable default values
    private StringParameter publisherParam;
    private StringParameter languageParam;
    private StringParameter formatParam;
    private StringParameter rightsNameParam;
    private StringParameter rightsUriParam;


    /**
     * Constructor
     */
    public EUROSTATETL()
    {
        super(new EUROSTATExtractor(), new EUROSTATTransformer());
    }

    //TODO: add or remove EventListeners?
    //TODO: Do we need createTransformer- or createExtractor-methods.

    @Override
    protected void registerParameters()
    {
        super.registerParameters();

        // define parameter mapping functions
        final Function<String, String> stringMappingFunction =
            ParameterMappingFunctions.createMapperForETL(ParameterMappingFunctions::mapToString, this);

        final Function<String, String> urlMappingFunction =
            ParameterMappingFunctions.createMapperForETL(ParameterMappingFunctions::mapToUrlString, this);

        // register parameters
        // Structural Definition Exchange Message (SDEM)
        this.sdemUrlParam = Configuration.registerParameter(
                                new StringParameter(
                                    EUROSTATConstants.SDEM_URL_KEY,
                                    getName(),
                                    EUROSTATConstants.SDEM_URL_DEFAULT_VALUE,
                                    urlMappingFunction));

        this.logoUrlParam = Configuration.registerParameter(
                                new StringParameter(
                                    EUROSTATConstants.LOGO_URL_KEY,
                                    getName(),
                                    EUROSTATConstants.LOGO_URL_DEFAULT_VALUE,
                                    stringMappingFunction));

        this.publisherParam = Configuration.registerParameter(
                                  new StringParameter(
                                      EUROSTATConstants.PUBLISHER_KEY,
                                      getName(),
                                      EUROSTATConstants.PUBLISHER_DEFAULT_VALUE,
                                      stringMappingFunction));

        this.languageParam = Configuration.registerParameter(
                                 new StringParameter(
                                     EUROSTATConstants.LANGUAGE_KEY,
                                     getName(),
                                     EUROSTATConstants.LANGUAGE_DEFAULT_VALUE,
                                     stringMappingFunction));

        this.formatParam = Configuration.registerParameter(
                               new StringParameter(
                                   EUROSTATConstants.FORMAT_KEY,
                                   getName(),
                                   EUROSTATConstants.FORMAT_DEFAULT_VALUE,
                                   stringMappingFunction));

        this.rightsNameParam = Configuration.registerParameter(
                                   new StringParameter(
                                       EUROSTATConstants.RIGHTS_NAME_KEY,
                                       getName(),
                                       EUROSTATConstants.RIGHTS_NAME_DEFAULT_VALUE,
                                       stringMappingFunction));

        this.rightsUriParam = Configuration.registerParameter(
                                  new StringParameter(
                                      EUROSTATConstants.RIGHTS_URI_KEY,
                                      getName(),
                                      EUROSTATConstants.RIGHTS_URI_DEFAULT_VALUE,
                                      stringMappingFunction));
    }

    /**
     * Getter for the Structural Data Exchange Message (SDEM).
     * The URL is directly retrieved from the corresponding parameter or from the default value.
     *
     * @return a SDEM URL, e.g. http://ec.europa.eu/eurostat/SDMX/diss-web/rest/dataflow/ESTAT/all/latest
     */
    public URL getSdemUrl() throws IllegalStateException
    {
        try {
            return new URL(this.sdemUrlParam.getValue());
        } catch (MalformedURLException e) {
            throw new IllegalStateException(EUROSTATConstants.MALFORMED_SDEM_URL_ERROR);
        }
    }

    /**
     * Getter for the URL that should point to the repository provider logo.
     * The URL is directly retrieved from the corresponding parameter.
     *
     * @return a Logo URL, e.g. http://ec.europa.eu/eurostat/SDMX/diss-web/rest/dataflow/ESTAT/all/latest
     */
    public String getLogoUrl()
    {
        return this.logoUrlParam.getValue();
    }

    /**
     * Getter for the default DataCite publisher value
     * The value is directly retrieved from the corresponding parameter
     * or from the default value.
     *
     * @return the name of the Publisher
     */
    public String getPublisher()
    {
        return this.publisherParam.getValue();
    }

    /**
     * Getter for the default DataCite language value
     * The value is directly retrieved from the corresponding parameter
     * or from the default value.
     *
     * @return the name of the Publisher
     */
    public String getLanguage()
    {
        return this.languageParam.getValue();
    }

    /**
     * Getter for the default DataCite formats 
     * The (only) value is directly retrieved from the corresponding parameter
     * or from the default value.
     *
     * @return the formats value
     */
    public Set<String> getFormats()
    {
        formats = new HashSet<String>();
        formats.add(this.formatParam.getValue());
        return formats;
    }

    /**
     * Getter for the default rightsList
     *
     * @return a DataCiteJson representation of the rightsList
     */
    public Set<Rights> getRightsList()
    {
        rightsList = new HashSet<Rights>();
        rightsList.add(new Rights(
                    this.rightsNameParam.getValue()
                    getRightsName(),
                    "en-US",
                    this.rightsUriParam.getValue()));

        return rightsList;
    }
}
