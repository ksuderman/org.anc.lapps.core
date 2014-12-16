package org.anc.lapps.core;

import org.anc.index.api.Index;
import org.anc.io.UTF8Reader;
import org.lappsgrid.api.Data;
import org.lappsgrid.api.DataSource;
import org.lappsgrid.core.DataFactory;
import org.lappsgrid.discriminator.Constants;
import org.lappsgrid.discriminator.DiscriminatorRegistry;
import org.lappsgrid.discriminator.Types;
import org.lappsgrid.discriminator.Uri;
import static org.lappsgrid.discriminator.Helpers.type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;


/**
 * @author Keith Suderman
 */
public abstract class AbstractDataSource implements DataSource
{
   private final Logger logger = LoggerFactory.getLogger(AbstractDataSource.class);
   protected Index index;
   protected Throwable savedException;
   private static final Map<String,String> extensionMap = new HashMap<String,String>();

   public AbstractDataSource(Index index)
   {
      this.index = index;
      synchronized(extensionMap) {
         if (extensionMap.size() == 0) {
            extensionMap.put("txt", Constants.Uri.TEXT);
            extensionMap.put("xml", Constants.Uri.XML);
            extensionMap.put("hdr", Constants.Uri.XML);
            extensionMap.put("json", Constants.Uri.JSON);
            extensionMap.put("jsonld", Constants.Uri.JSON_LD);
         }
      }
   }

//   @Override
   public long size(Data data)
   {

      return index.keys().size();
   }


   @Override
   public Data list(Data data)
   {
//
//      String list = collect(index.keys().subList(start, end));
//      return new Data(Uri.STRING_LIST, list);
      return DataFactory.error("Not implemented");
   }

   @Override
   public Data get(String key)
   {
      logger.info("Getting document for {}", key);
      File file = index.get(key);
      if (file == null)
      {
         logger.error("No such file.");
         return DataFactory.error("No such file.");
      }

      if (!file.exists())
      {
         logger.error("File not found.");
         return DataFactory.error("File not found.");
      }

      UTF8Reader reader = null;
      Data result = null;
      String type = getFileType(file);
      try
      {
         logger.debug("Loading {}", file.getPath());

         reader = new UTF8Reader(file);
         String text = reader.readString();
         reader.close();
         result = new Data(type, text);
      }
      catch (IOException e)
      {
         logger.error("Unable to load file.", e);
         result = DataFactory.error(e.getMessage());
      }
      logger.debug("Returning the Data object.");
      return result;
   }


   @Override
   public Data query(Data query)
   {
      if (savedException != null)
      {
         return DataFactory.error(savedException.getMessage());
      }
      long type = type(query);
      if (type == Types.GET)
      {
         return get(query.getPayload());
      }
      if (type == Types.LIST)
      {
         return list(query);
      }
      return DataFactory.error("Unsupported operation.");
   }

   /**
    * Determines the type of file based on it file extension.
    *
    * @param file the file to check
    * @return a discriminator value based on the file's
    * extension.
    */
   protected String getFileType(File file)
   {
      String filename = file.getName();
      int dot = filename.lastIndexOf('.');
      if (dot <= 0)
      {
         return Uri.TEXT;
      }
      String type = extensionMap.get(filename.substring(dot+1));
      if (type == null)
      {
         return Uri.TEXT;
      }
      return type;
   }

   protected Data doQuery(String queryString)
   {
      List<String> list = new ArrayList<String>();
      for (String key : index.keys())
      {
         File file = index.get(key);
         if (file.getPath().contains(queryString))
         {
            list.add(key);
         }
      }
      return DataFactory.index(collect(list));
   }

   /**
    * Takes a list of String objects and concatenates them into
    * a single String. Items in the list are separated by a single
    * space character.
    *
    */
   private String collect(Collection<String> list)
   {
      StringBuilder buffer = new StringBuilder();
      Iterator<String> it = list.iterator();
      if (it.hasNext())
      {
         buffer.append(it.next());
      }
      while (it.hasNext())
      {
         buffer.append(' ');
         buffer.append(it.next());
      }
      return buffer.toString();
   }

}
