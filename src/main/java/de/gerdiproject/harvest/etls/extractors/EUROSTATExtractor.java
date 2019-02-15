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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import org.sdmxsource.sdmx.api.constants.SDMX_STRUCTURE_TYPE;
import org.sdmxsource.sdmx.api.model.StructureWorkspace;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.beans.reference.StructureReferenceBean;
import org.sdmxsource.sdmx.structureparser.manager.parsing.impl.StructureParsingManagerImpl;
import org.sdmxsource.util.io.ReadableDataLocationTmp;

import de.gerdiproject.harvest.etls.AbstractETL;
import de.gerdiproject.harvest.etls.EUROSTATETL;
import de.gerdiproject.harvest.etls.SDMXDataChunk;

/**
 * This {@linkplain AbstractIteratorExtractor} implementation extracts all
 * (meta-)data from EUROSTAT and bundles it into a {@linkplain SDMXDataChunk}.
 *
 * @author Tobias Weber
 */
public class EUROSTATExtractor extends AbstractIteratorExtractor<SDMXDataChunk>
{
    private String version = null;
    private int size = -1;
    private StructureParsingManagerImpl parser = new StructureParsingManagerImpl();
    private StructureWorkspace sdem = null;

    @Override
    public void init(AbstractETL<?, ?> etl)
    {
        super.init(etl);

        final EUROSTATETL eurostatEtl = (EUROSTATETL) etl;
        sdem = parser.parseStructures(new ReadableDataLocationTmp(eurostatEtl.getSdemUrl()));
        size = sdem.getStructureBeans(false).getDataflows().size();
        version = sdem.getStructureBeans(false).getHeader().getId();
    }

    @Override
    public String getUniqueVersionString()
    {
        return version;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    protected Iterator<SDMXDataChunk> extractAll() throws ExtractorException
    {
        return new EUROSTATIterator(this.sdem.getStructureBeans(false).getDataflows());
    }

    /**
     * This iterator iterates over all dataflows in the sdem and retrieves the
     * data structure message
     *
     * @author Tobias Weber
     */
    private class EUROSTATIterator implements Iterator<SDMXDataChunk>
    {
        private Queue<DataflowBean> dataflows = new LinkedList<>();

        /**
         * Adds a set of DataflowBeans to the harvest queue
         *
         * @param dataflows set of DataflowBeans to be added to the queue
         */
        public EUROSTATIterator(Set<DataflowBean> dataflows)

        {
            dataflows.addAll(dataflows);
        }

        @Override
        public boolean hasNext()
        {
            return !dataflows.isEmpty();
        }

        @Override
        public SDMXDataChunk next()
        {
            final DataflowBean dataflowBean = dataflows.remove();
            StructureReferenceBean structureReferenceBean
                = dataflowBean.getDataStructureRef().createMutableInstance();

            if (structureReferenceBean.getMaintainableStructureType()
                == SDMX_STRUCTURE_TYPE.DATASOURCE) {
                SDMXDataChunk sdmxDataChunk = new SDMXDataChunk(
                    dataflowBean.getNames(),
                    (DataStructureBean) structureReferenceBean.getMaintainableReference());
                return sdmxDataChunk;
            } else
                return next();
        }
    }
}
