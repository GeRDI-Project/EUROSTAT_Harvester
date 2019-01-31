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
    /**
     * Constructor
     */
    public EUROSTATETL()
    {
        super(new EUROSTATExtractor(), new EUROSTATTransformer());
    }

    // TODO 1. Check if StaticIteratorETL really suits your needs, or exchange it with any other AbstractETL.
    // TODO 2. Exchange EUROSTATVO with whatever is extracted from your DataProvider or populate it with fitting data.
    // TODO 3. Override registerParameters() if you need to register additional ETL parameters.
    // TODO 4. Override any other methods if needed.
}
