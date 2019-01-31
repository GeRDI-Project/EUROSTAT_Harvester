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

import de.gerdiproject.harvest.etls.AbstractETL;
import de.gerdiproject.harvest.utils.data.HttpRequester;

/**
 * This {@linkplain AbstractIteratorExtractor} implementation extracts all
 * (meta-)data from EUROSTAT and bundles it into a {@linkplain EUROSTATVO}.
 *
 * @author Tobias Weber
 */
public class EUROSTATExtractor extends AbstractIteratorExtractor<EUROSTATVO>
{
    private final HttpRequester httpRequester;

    private String version = null;
    private int size = -1;


    /**
     * Simple constructor.
     */
    public EUROSTATExtractor()
    {
        this.httpRequester = new HttpRequester();
    }


    @Override
    public void init(AbstractETL<?, ?> etl)
    {
        super.init(etl);

        this.httpRequester.setCharset(etl.getCharset());

        // TODO if possible, extract some metadata in order to determine the size and a version string
        // final EUROSTATETL specificEtl = (EUROSTATETL) etl;
        // this.version = ;
        // this.size = ;
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
    protected Iterator<EUROSTATVO> extractAll() throws ExtractorException
    {
        return new EUROSTATIterator();
    }


    /**
     * TODO add a description here
     *
     * @author Tobias Weber
     */
    private class EUROSTATIterator implements Iterator<EUROSTATVO>
    {
        @Override
        public boolean hasNext()
        {
            // TODO
            return false;
        }


        @Override
        public EUROSTATVO next()
        {
            // TODO
            return null;
        }
    }
}
