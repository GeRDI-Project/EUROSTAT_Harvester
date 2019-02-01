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

import de.gerdiproject.harvest.etls.extractors.EUROSTATExtractor;
import de.gerdiproject.harvest.etls.extractors.EUROSTATVO;
import de.gerdiproject.harvest.etls.transformers.EUROSTATTransformer;
import de.gerdiproject.json.datacite.DataCiteJson;


/**
 * An ETL for harvesting EUROSTAT.<br>
 * See: https://ec.europa.eu/eurostat
 *
 * @author Tobias Weber
 */
public class EUROSTATETL extends StaticIteratorETL<EUROSTATVO, DataCiteJson>
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
                    EUROSTATParameterConstants.SDEM_URL_KEY,
                    getName(),
                    EUROSTATParameterConstants.SDEM_URL_DEFAULT_VALUE,
                    urlMappingFunction));

        this.logoUrlParam = Configuration.registerParameter(
                new StringParameter(
                    EUROSTATParameterConstants.LOGO_URL_KEY,
                    getName(),
                    EUROSTATParameterConstants.LOGO_URL_DEFAULT_VALUE,
                    stringMappingFunction));

        this.publisherParam = Configuration.registerParameter(
                new StringParameter(
                    EUROSTATParameterConstants.PUBLISHER_KEY,
                    getName(),
                    EUROSTATParameterConstants.PUBLISHER_DEFAULT_VALUE,
                    stringMappingFunction));

        this.languageParam = Configuration.registerParameter(
                new StringParameter(
                    EUROSTATParameterConstants.LANGUAGE_KEY,
                    getName(),
                    EUROSTATParameterConstants.LANGUAGE_DEFAULT_VALUE,
                    stringMappingFunction));

        this.formatParam = Configuration.registerParameter(
                new StringParameter(
                    EUROSTATParameterConstants.FORMAT_KEY,
                    getName(),
                    EUROSTATParameterConstants.FORMAT_DEFAULT_VALUE,
                    stringMappingFunction));

        this.rightsNameParam = Configuration.registerParameter(
                new StringParameter(
                    EUROSTATParameterConstants.RIGHTS_NAME_KEY,
                    getName(),
                    EUROSTATParameterConstants.RIGHTS_NAME_DEFAULT_VALUE,
                    stringMappingFunction));

        this.rightsUriParam = Configuration.registerParameter(
                new StringParameter(
                    EUROSTATParameterConstants.RIGHTS_URI_KEY,
                    getName(),
                    EUROSTATParameterConstants.RIGHTS_URI_DEFAULT_VALUE,
                    stringMappingFunction));
    }

    /**
     * Getter for the Structural Data Exchange Message (SDEM).
     * The URL is directly retrieved from the corresponding parameter or from the default value.
     *
     * @return a SDEM URL, e.g. http://ec.europa.eu/eurostat/SDMX/diss-web/rest/dataflow/ESTAT/all/latest
     */
    public String getSdemUrl()
    {
        return this.sdemUrl;
    }

    /**
     * Getter for the URL that should point to the repository provider logo.
     * The URL is directly retrieved from the corresponding parameter.
     *
     * @return a SDEM URL, e.g. http://ec.europa.eu/eurostat/SDMX/diss-web/rest/dataflow/ESTAT/all/latest
     */
    public String getSdemUrl()
    {
        return this.sdemUrl;
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
        return this.publisherParam;
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
        return this.languageParam;
    }

    /**
     * Getter for the default DataCite format value
     * The value is directly retrieved from the corresponding parameter
     * or from the default value.
     *
     * @return the format value
     */
    public String getFormat()
    {
        return this.formatParam;
    }

    /**
     * Getter for the default DataCite rightsName value
     * The value is directly retrieved from the corresponding parameter
     * or from the default value.
     *
     * @return the name of the data license
     */
    public String getRightsName()
    {
        return this.rightsNameParam;
    }

    /**
     * Getter for the default DataCite rightsUri value
     * The value is directly retrieved from the corresponding parameter
     * or from the default value.
     *
     * @return the uri of the data license
     */
    public String getRightsUri()
    {
        return this.rightsUriParam;
    }

    // TODO 1. Check if StaticIteratorETL really suits your needs, or exchange it with any other AbstractETL.
    // TODO 2. Exchange EUROSTATVO with whatever is extracted from your DataProvider or populate it with fitting data.
}
