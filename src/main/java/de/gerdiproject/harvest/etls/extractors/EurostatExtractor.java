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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.sdmxsource.sdmx.api.manager.parse.StructureParsingManager;
import org.sdmxsource.sdmx.api.model.StructureWorkspace;
import org.sdmxsource.sdmx.api.model.beans.base.SDMXBean;
import org.sdmxsource.sdmx.api.model.beans.codelist.CodeBean;
import org.sdmxsource.sdmx.api.model.beans.codelist.CodelistBean;
import org.sdmxsource.sdmx.api.model.beans.conceptscheme.ConceptBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DataflowBean;
import org.sdmxsource.sdmx.api.model.beans.datastructure.DimensionBean;
import org.sdmxsource.sdmx.api.model.beans.reference.CrossReferenceBean;
import org.sdmxsource.sdmx.api.model.superbeans.datastructure.DataStructureSuperBean;
import org.sdmxsource.sdmx.api.util.ReadableDataLocation;
import org.sdmxsource.util.factory.SdmxSourceReadableDataLocationFactory;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import de.gerdiproject.harvest.etls.AbstractETL;
import de.gerdiproject.harvest.etls.EurostatETL;
import de.gerdiproject.harvest.etls.SdmxUtil;
import de.gerdiproject.harvest.eurostat.constants.EurostatConstants;



/**
 * This {@linkplain AbstractIteratorExtractor} implementation extracts all
 * (meta-)data from Eurostat and bundles it into a {@linkplain SdmxVO}.
 *
 * @author Tobias Weber
 */
public class EurostatExtractor extends AbstractIteratorExtractor<SdmxVO>
{
    private String version = null;
    private int size = -1;


    private StructureParsingManager parser;
    private StructureWorkspace sdem;
    private SdmxSourceReadableDataLocationFactory rdlFactory;

    @Override
    public void init(AbstractETL<?, ?> etl)
    {
        super.init(etl);
        final EurostatETL eurostatEtl = (EurostatETL) etl;

        //This nonsense is the only way to avoid a NullPointerException that I (weber@lrz.de) found.
        //We need to let spring initialise the specific class in order to initialise them correctly
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("spring/beans.xml");
        rdlFactory = (SdmxSourceReadableDataLocationFactory) context.getBean("readableDataLocationFactory");
        parser = (StructureParsingManager) context.getBean("structureParsingManager");

        ReadableDataLocation rdl = rdlFactory.getReadableDataLocation(eurostatEtl.getSdemUrl());
        sdem = parser.parseStructures(rdl);
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
    protected Iterator<SdmxVO> extractAll() throws ExtractorException
    {
        return new EurostatIterator(this.sdem.getStructureBeans(false).getDataflows(),
                                    this.rdlFactory,
                                    this.parser);
    }

    /**
     * Get a list of dimension values which are both configured and present in the data structure
     *
     * @param the SdmxVO in question
     *
     * @return a list of all possible combinations of codes of each allowed and present dimension
     */
    public static List<Map<String, CodeBean>> getDimensionCombinations(
        DataStructureBean dataStructureBean)
    {
        HashMap<String, List<CodeBean>> input = new HashMap<String, List<CodeBean>>();

        // get all dimensions that are allowed AND existent in source
        for (DimensionBean dimensionBean : dataStructureBean.getDimensions()) {
            final String id = dimensionBean.getId();

            if (EurostatConstants.ALLOWED_DIMENSIONS.contains(id))
                input.put(id, getCodeList(dataStructureBean, id));
        }

        //Build a list of all possible combinations of values of each dimension.
        return SdmxUtil.mapOfListsToListOfMaps(input);
    }

    /**
     * Get a list of all Codes given a dimensionId
     *
     * @param source the SdmxVO to be searched
     * @param dimensionId the ID of the dimension to be iterated over
     *
     * @return A list of all code values for the dimension in source
     */
    public static List<CodeBean> getCodeList(
        DataStructureBean dataStructureBean, String dimensionId)
    {
        LinkedList<CodeBean> codes = new LinkedList<CodeBean>();

        DimensionBean dimensionBean = dataStructureBean.getDimension(dimensionId);
        List<CrossReferenceBean> conceptRole = dimensionBean.getConceptRole();
        ConceptBean conceptBean
            = (ConceptBean) conceptRole.get(0).createMutableInstance().getMaintainableReference();

        CodelistBean codeListBean
            = (CodelistBean) conceptBean.getCoreRepresentation()
              .getRepresentation()
              .createMutableInstance()
              .getMaintainableReference();

        for (SDMXBean code : codeListBean.getItems())
            codes.add((CodeBean) code);

        return codes;
    }


    /**
     * This iterator iterates over all dataflows in the sdem and retrieves the
     * sdmx value objects.
     *
     * @author Tobias Weber
     */
    private static class EurostatIterator implements Iterator<SdmxVO>
    {
        private Queue<DataflowBean> dataflows = new LinkedList<>();
        private Queue<SdmxVO> chunkedDataflow = new LinkedList<>();
        private SdmxSourceReadableDataLocationFactory rdlFactory;
        private StructureParsingManager parser;

        /**
         * Adds a set of DataflowBeans to the harvest queue
         *
         * @param dataflows set of DataflowBeans to be added to the queue
         */
        public EurostatIterator(Set<DataflowBean> dataflows,
                                SdmxSourceReadableDataLocationFactory rdlFactory,
                                StructureParsingManager parser)
        {
            this.dataflows.addAll(dataflows);
            this.rdlFactory = rdlFactory;
            this.parser = parser;
        }

        @Override
        public boolean hasNext()
        {
            return !(dataflows.isEmpty() && chunkedDataflow.isEmpty());
        }

        @Override
        public SdmxVO next()
        {
            if (chunkedDataflow.isEmpty()) {
                //According to the documentation, the "right" way to retrieve all the DataStructures
                //would be via a parseStructures(rdl, rds, rdm)-call that uses RESTSdmxBeanRetrievalManager
                //in init(). Unfortunately the RESTSdmxBeanRetrievalManager is throwing
                //NullPointerExceptions. This is a workaround until the problem could be solved
                //or the source code is available to see WHY these exceptions are thrown.
                final DataflowBean dataflowBean = dataflows.remove();
                String url = String.format(EurostatConstants.SDMX_BASE_URL_FORMAT,
                                           dataflowBean.getDataStructureRef().getMaintainableId());
                ReadableDataLocation rdl = rdlFactory.getReadableDataLocation(url);
                StructureWorkspace workspace = parser.parseStructures(rdl);

                DataStructureSuperBean dataStructureSuperBean =
                    (DataStructureSuperBean) workspace.getSuperBeans()
                    .getDataStructures().toArray()[0];
                DataStructureBean dataStructureBean = dataStructureSuperBean.getBuiltFrom();

                //get all dimensions from the DataStructureBean, filling up chunkedDataflow
                for (Map<String, CodeBean> dimensionCombination :
                     getDimensionCombinations(dataStructureBean)) {
                    chunkedDataflow.add(new SdmxVO(
                                            dataflowBean.getNames(),
                                            dataStructureBean,
                                            dimensionCombination));
                }

            }

            return chunkedDataflow.remove();
        }
    }
}
