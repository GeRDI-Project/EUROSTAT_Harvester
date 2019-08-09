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

package de.gerdiproject.harvest.etls.extractors;

import java.util.List;
import java.util.Map;

import org.sdmxsource.sdmx.api.model.beans.base.TextTypeWrapper;
import org.sdmxsource.sdmx.api.model.beans.codelist.CodeBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;

import lombok.Value;

/**
 * This class is a value object that contains all extracted (meta-) data from
 * Eurostat that is required to generate a document.
 *
 * @author Tobias Weber
 */
@Value
public class SdmxVO
{
    /**
     * List of names for the dataflow in different locales
     */
    private final List<TextTypeWrapper> names;

    /**
     * DataStructureBean with dimensions and codelists
     */
    private final DataStructureBean dataStructureBean;

    private final Map<String, CodeBean> dimensions;


    /**
     * Get the English name, if it exists, if not pick the first one
     *
     * @return containing the name of the Dataflow, which the SdmxVO describes
     */
    public String getEnglishOrFirstName()
    {
        for (final TextTypeWrapper text : this.names) {
            if (text.getLocale().equals("en"))
                return text.getValue();
        }

        return this.names.get(0).getValue();
    }
}
