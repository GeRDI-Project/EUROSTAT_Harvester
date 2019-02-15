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

import java.util.List;

import org.sdmxsource.sdmx.api.model.beans.base.TextTypeWrapper;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;

/**
 * This {@linkplain SDMXDataChunk} is a container for a SDMX dataflow with
 * exactly one referenced SDMX data structure.
 * It is a composite of a List of names for the dataflow in different locales
 * and a {@linkplain DataStructureBean}
 *
 * @author Tobias Weber
 */
public class SDMXDataChunk
{
    private List<TextTypeWrapper> names;
    private DataStructureBean dataStructureBean;

    /**
     * Constructor
     *
     * @param TextTypeWrapper List of names for the dataflow in different locales
     * @param DataStructureBean DataStructureBean with dimensions and codelists.
     */
    public SDMXDataChunk(List<TextTypeWrapper> names,
                         DataStructureBean dataStructureBean)
    {
        this.names = names;
        this.dataStructureBean = dataStructureBean;
    }

    /**
     * Getter for the DataStructureBean
     *
     * @return The DataStructureBean
     */
    public DataStructureBean getDataStructureBean()
    {
        return this.dataStructureBean;
    }

    /**
     * Get the English name, if it exists, if not pick the first one
     *
     * @return containing the name of the Dataflow, which the SDMXDataChunkdescribes
     */
    public String getEnglishOrFirstName()
    {
        for (TextTypeWrapper text : this.names) {
            if (text.getLocale().equals("en"))
                return text.getValue();
        }

        return this.names.get(0).getValue();
    }
}
