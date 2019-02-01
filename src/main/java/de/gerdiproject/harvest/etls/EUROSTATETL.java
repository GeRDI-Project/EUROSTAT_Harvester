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
    // URL
    private StringParameter structuralDefinitionExchangeMessageUrl;
    private StringParameter publisher;
    private StringParameter language;
    private StringParameter format;
    private StringParameter rightsName;
    private StringParameter rightsURI;

    /**
     * Constructor
     */
    public EUROSTATETL()
    {
        super(new EUROSTATExtractor(), new EUROSTATTransformer());
    }

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
        this.structuralDefinitionExchangeMessageUrl = Configuration.registerParameter(
                new StringParameter(
                    EUROSTATParameterConstants.SDEM_URL_KEY,
                    getName(),
                    EUROSTATParameterConstants.SDEM_URL_DEFAULT_VALUE,
                    stringMappingFunction));
        
        this.publisher = Configuration.registerParameter(
                new StringParameter(
                    EUROSTATParameterConstants.PUBLISHER_KEY,
                    getName(),
                    EUROSTATParameterConstants.PUBLISHER_DEFAULT_VALUE,
                    stringMappingFunction));

        this.language = Configuration.registerParameter(
                new StringParameter(
                    EUROSTATParameterConstants.LANGUAGE_KEY,
                    getName(),
                    EUROSTATParameterConstants.LANGUAGE_DEFAULT_VALUE,
                    stringMappingFunction));

        this.format = Configuration.registerParameter(
                new StringParameter(
                    EUROSTATParameterConstants.FORMAT_KEY,
                    getName(),
                    EUROSTATParameterConstants.FORMAT_DEFAULT_VALUE,
                    stringMappingFunction));

        this.rightsName = Configuration.registerParameter(
                new StringParameter(
                    EUROSTATParameterConstants.RIGHTS_NAME_KEY,
                    getName(),
                    EUROSTATParameterConstants.RIGHTS_NAME_DEFAULT_VALUE,
                    stringMappingFunction));

        this.rightsURI = Configuration.registerParameter(
                new StringParameter(
                    EUROSTATParameterConstants.RIGHTS_URI_KEY,
                    getName(),
                    EUROSTATParameterConstants.RIGHTS_URI_DEFAULT_VALUE,
                    stringMappingFunction));
    }

    // TODO 1. Check if StaticIteratorETL really suits your needs, or exchange it with any other AbstractETL.
    // TODO 2. Exchange EUROSTATVO with whatever is extracted from your DataProvider or populate it with fitting data.
    // TODO 3. Override registerParameters() if you need to register additional ETL parameters.
    // TODO 4. Override any other methods if needed.
}
