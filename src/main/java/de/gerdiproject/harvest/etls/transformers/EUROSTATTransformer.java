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

import java.util.Calendar;

import org.sdmxsource.sdmx.api.model.beans.datastructure.DataStructureBean;

import de.gerdiproject.harvest.etls.AbstractETL;
import de.gerdiproject.harvest.etls.sdmx.DataStructure;
import de.gerdiproject.json.datacite.DataCiteJson;

/**
 * This transformer parses metadata from a {@linkplain DataStructureBean}
 * and creates {@linkplain LinkedList<DataCiteJson>} objects from it.
 *
 * @author Tobias Weber
 */
public class EUROSTATTransformer extends AbstractIteratorTransformer<DataStructure, LinkedList<DataCiteJson>>
{

    @Override
    public void init(AbstractETL<?, ?> etl)
    {
        super.init(etl);
    }

    @Override
    protected LinkedList<DataCiteJson> transformElement(DataStructure source)
    {
        LinkedList<DataCiteJson> records = new LinkedList<DataCiteJson>();
       
        LinkedList<HashMap<String, String>> dimensionSelections = getDimensionSelections(); 

        for(HashMap<String,String> dimensionSelection: dimenstionSelections)
        { 
            records.add(getDocument(source, dimensionSelectionl));
        }
        return records;
    }

    /**
     * Get a list of dimension values which are both configured and present in the data structure
     *
     * @param the DataStructure in question
     *
     * @return a list containing a hash with names of present dimensions as keys and their code as value.
     */
    private List<Map<String, String>> getDimensionSelections(DataStructure source)
    {
        List<String> dimensionNames = new LinkedList<String>();

        for(DimensionBean db : source.getDataStructureBean.getDimensions())
        {
            if(getAllowedDimensionNames().contains(db.getId()) {
                dimensionNames.add(db.getId());
            }
        }
        //iterate over all dimensions CONTINUE HERE

    }

    /**
     * A list of dimensions that are supported
     *
     * @return list of names of dimensions as strings
     */
    private List<String> getAllowedDimensionNames()
    {
        return new ArrayList<String>() {
            {
                add("NA_ITEM");
                add("GEO");
                add("UNIT");
            }
        };
    }

    private DataCiteJson getDocument(DataStructure source, 
            Hash<String, String> dimensionSelection)
    {
        Identifier identifier = new Identifier(getIdentifier(source, dimensionSelection)); 
        DataCiteJson document = createDataCiteStub(identifier); 
        document.addTitles(getTitle(source, dimenstionSelection));
        document.addSubjects(getSubjects(source, dimensionSelection));
        document.addDescriptions(getDescription(source, dimensionSelection));
        if (hasGeoDimension(source)) {
            documentaddGeoLocations(getGeoLocations(source));
        }
        document.setResearchDataList(getResearchDataList(source), identifier);

    }

    /**
     * Creates a stub for a DataCiteJson object (with fields that are
     * identical for all documents
     *
     * @param Identifier identifier for the DataCiteJson
     *
     * @return the DataCiteJson document
     */
    private DataCiteJson createDataCiteStub(Identifier identifier)
    {
        DataCiteJson document = new DataCiteJson(identifier); 

        document.setPublisher(etl.getPublisher());
        document.setPublicationYear(
                String.valueOf(Calender.getInstance().get(Calender.Year)));
        document.setLanguage(etl.getLanguage());
        document.setResourceType(
                new ResourceType("Statistical Data", ResourceTypeGeneral.Dataset));
        document.setFormats(etl.getFormats());
        document.setRightsList(etl.getRightsList());

        return document;
    }



    /**
     * Creates a unique identifier for a document from EUROSTAT.
     *
     * @param source the source object that contains all metadata that is needed
     *
     * @return a unique identifier of this document
     */
    private String createIdentifier(DataStructureBean source)
    {
        // TODO retrieve a unique identifier from the source
        return source.toString();
    }
}
