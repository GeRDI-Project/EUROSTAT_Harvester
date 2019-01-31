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

import de.gerdiproject.harvest.etls.AbstractETL;
import de.gerdiproject.harvest.etls.extractors.EUROSTATVO;
import de.gerdiproject.json.datacite.DataCiteJson;

/**
 * This transformer parses metadata from a {@linkplain EUROSTATVO}
 * and creates {@linkplain DataCiteJson} objects from it.
 *
 * @author Tobias Weber
 */
public class EUROSTATTransformer extends AbstractIteratorTransformer<EUROSTATVO, DataCiteJson>
{
    @Override
    public void init(AbstractETL<?, ?> etl)
    {
        super.init(etl);
    }


    @Override
    protected DataCiteJson transformElement(EUROSTATVO source)
    {
        // create the document
        final DataCiteJson document = new DataCiteJson(createIdentifier(source));

        // TODO add all possible metadata to the document

        return document;
    }


    /**
     * Creates a unique identifier for a document from EUROSTAT.
     *
     * @param source the source object that contains all metadata that is needed
     *
     * @return a unique identifier of this document
     */
    private String createIdentifier(EUROSTATVO source)
    {
        // TODO retrieve a unique identifier from the source
        return source.toString();
    }
}
