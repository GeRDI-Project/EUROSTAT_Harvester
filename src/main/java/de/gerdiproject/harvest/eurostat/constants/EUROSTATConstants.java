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
package de.gerdiproject.harvest.etls.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * A static collection of constant parameters regarding OAI-PMH.
 *
 * @author Tobias Weber
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EUROSTATConstants
{
    // URLs
    // Structural Data Exchange Message (SDEM)
    public static final String SDEM_URL_KEY = "sdemUrl";
    public static final String SDEM_URL_DEFAULT_VALUE
        = "http://ec.europa.eu/eurostat/SDMX/diss-web/rest/dataflow/ESTAT/all/latest";
    public static final String LOGO_URL_KEY = "logoUrl";
    public static final String LOGO_URL_DEFAULT_VALUE = "";

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
                                                          + EUROSTATConstants.SDEM_URL_KEY
                                                          + "'-parameter in the config!";
    public static final String NO_RECORDS_ERROR = "The URL '%s' did not yield any harvestable records! Change the parameters in the config!";
}
