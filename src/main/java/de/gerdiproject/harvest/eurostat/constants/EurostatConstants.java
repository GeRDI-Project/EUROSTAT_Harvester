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
package de.gerdiproject.harvest.eurostat.constants;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * A static collection of constant parameters regarding OAI-PMH.
 *
 * @author Tobias Weber
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EurostatConstants
{
    // URLs
    // Structural Data Exchange Message (SDEM)
    public static final String SDEM_URL_KEY = "sdemUrl";
    public static final String SDEM_URL_DEFAULT_VALUE
        = "http://ec.europa.eu/eurostat/SDMX/diss-web/rest/dataflow/ESTAT/all/latest";
    public static final String REST_URL_BASE_KEY = "restUrlBase";
    public static final String REST_URL_BASE_DEFAULT_VALUE
        = "http://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en";
    public static final String LOGO_URL_KEY = "logoUrl";
    public static final String LOGO_URL_DEFAULT_VALUE = "";

    public static final String SDMX_BASE_URL_FORMAT
        = "http://ec.europa.eu/eurostat/SDMX/diss-web/rest/datastructure/ESTAT/%s";

    // Metadata default values
    public static final String PUBLISHER_KEY = "publisher";
    public static final String PUBLISHER_DEFAULT_VALUE = "Eurostat";
    public static final String LANGUAGE_KEY = "language";
    public static final String LANGUAGE_DEFAULT_VALUE = "en";
    public static final String FORMAT_KEY = "format";
    public static final String FORMAT_DEFAULT_VALUE = "application/json";
    public static final String RIGHTS_NAME_KEY = "rightsName";
    public static final String RIGHTS_NAME_DEFAULT_VALUE = "Eurostat License";
    public static final String RIGHTS_URI_KEY = "rightsUri";
    public static final String RIGHTS_URI_DEFAULT_VALUE
        = "https://ec.europa.eu/eurostat/about/policies/copyright";

    // Errors
    public static final String CANNOT_HARVEST = "Cannot harvest: ";
    public static final String CANNOT_CREATE_TRANSFORMER = "Cannot create transformer!";
    public static final String MALFORMED_SDEM_URL_ERROR = "You must correctly set the '"
                                                          + EurostatConstants.SDEM_URL_KEY
                                                          + "'-parameter in the config!";
    public static final String NO_RECORDS_ERROR = "The URL '%s' did not yield any harvestable records! Change the parameters in the config!";

    //MISC
    public static final String GEO_DIMENSION = "GEO";
    public static final String ALLOWED_DIMENSIONS_KEY = "allowedDimensions";
    public static final String ALLOWED_DIMENSIONS_DEFAULT_VALUE = "NA_ITEM,GEO,UNIT,FREQ,INDICATORS,PARTNER";

    public static final String DATA_PRODUCT_REGEX_KEY = "dataProductRegex";
    public static final String DATA_PRODUCT_REGEX_DEFAULT_VALUE = "DSD_.*"; 

    public static final String TITLE_FORMAT = "%s (%s)";
    public static final String TITLE_DIMENSION_FORMAT = "%s: %s";
    public static final String TITLE_DIMENSION_SEPARATOR = ", ";

    public static final String IDENTIFIER_FORMAT = "%s/%s?%s";

    public static final String DESCRIPTION_FORMAT = "%s%n%s";
    public static final char DESCRIPTION_DIMENSION_SEPARATOR = '\n';

    public static final String QUERY_PARAM_FORMAT = "%s=%s";
    public static final char QUERY_PARAM_SEPARATOR = '&';


}
