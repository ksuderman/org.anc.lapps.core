package org.anc.lapps.core;

import org.anc.index.api.Index;
import org.anc.io.UTF8Reader;
import org.lappsgrid.discriminator.Constants;
import org.lappsgrid.discriminator.DiscriminatorRegistry;
import org.lappsgrid.serialization.*;
import org.lappsgrid.serialization.Error;
import org.lappsgrid.experimental.api.WebService;
import org.lappsgrid.serialization.Error;
import org.lappsgrid.serialization.response.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;


/**
 * @author Keith Suderman
 */
public abstract class AbstractDataSource implements WebService
{
   private final Logger logger = LoggerFactory.getLogger(AbstractDataSource.class);
   protected Index index;
   protected Throwable savedException;
   private static final Map<String,String> extensionMap = new HashMap<String,String>();
   private final String metadata;

   public AbstractDataSource(Index index, String metadata)
   {
      this.index = index;
      this.metadata = metadata;

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

   public String execute(String input)
   {
      Map<String,Object> map = Serializer.parse(input, HashMap.class);
      String discriminator = (String) map.get("discriminator");
      if (discriminator == null)
      {
         return Serializer.toJson(new Error("No discriminator value provided."));
      }

      String result = null;
      switch (discriminator)
      {
         case Constants.Uri.SIZE:
            Data<Integer> sizeData = new Data<Integer>();
            sizeData.setDiscriminator(Constants.Uri.OK);
            sizeData.setPayload(index.keys().size());
            result = Serializer.toJson(sizeData);
            break;
         case Constants.Uri.LIST:
            java.util.List<String> keys = index.keys();
            Object startValue = map.get("start");
            if (startValue != null)
            {
               int start = Integer.parseInt(startValue.toString());
               int end = index.keys().size();
               Object endValue = map.get("end");
               if (endValue != null)
               {
                  end = Integer.parseInt(endValue.toString());
               }
               keys = keys.subList(start, end);
            }
            Data<java.util.List<String>> listData = new Data<>();
            listData.setDiscriminator(Constants.Uri.OK);
            listData.setPayload(keys);
            result = Serializer.toJson(listData);
            break;
         case Constants.Uri.GET:
            String key = map.get("payload").toString();
            if (key == null)
            {
               result = error("No key value provided");
            }
            else
            {
               File file = index.get(key);
               if (file == null)
               {
                  result = error("No such file.");
               }
               else if (!file.exists())
               {
                  result = error("That file was not found on this server.");
               }
               else try
               {
                  UTF8Reader reader = new UTF8Reader(file);
                  String content = reader.readString();
                  reader.close();
                  Data<String> stringData = new Data<String>();
                  stringData.setDiscriminator(Constants.Uri.OK);
                  stringData.setPayload(content);
                  result = Serializer.toJson(stringData);
               }
               catch (IOException e)
               {
                  result = error(e.getMessage());
               }

            }
            break;
         case Constants.Uri.GETMETADATA:
            Data<String> data = new Data<String>();
            data.setDiscriminator(Constants.Uri.OK);
            data.setPayload(metadata);
            result = Serializer.toJson(data);
            break;
         default:
            result = error("Invalid discriminator: " + discriminator);
            break;
      }
      return result;
   }

   private String error(String message)
   {
      return Serializer.toJson(new Error(message));
   }
   /*
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
         return Constants.Uri.TEXT;
      }
      String type = extensionMap.get(filename.substring(dot+1));
      if (type == null)
      {
         return Constants.Uri.TEXT;
      }
      return type;
   }

   /*
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
   */

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
