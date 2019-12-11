package br.ufrj.ppgi.cores.adaptors.twitteradaptor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * @author Jivago Medeiros
 */
public class Adaptor {

    public static void main(String[] args) {
        
        //new HashMap<"parada-recorte-120.json", "tweets-parada.ttl">
        List<String[]> dataset_files = new ArrayList();
        
        //nome/caminho do arquivo de entrada e nome/caminho do arquivo de saída
        
        dataset_files.add(new String[]{"parada.json", "parada.ttl"});
        dataset_files.add(new String[]{"protestos2013-23A30-Jun.json", "protestos.ttl"});
        dataset_files.add(new String[]{"rocinha.json", "rocinha.ttl"});

        JSONParser jsonParser = new JSONParser();
        
        String twitter_url = "https://twitter.com/";

        for (String[] str : dataset_files) {

            String in_json_file_name = str[0]; 
            String out_ttl_file_name = str[1]; 
            
            try (Stream<String> lines = Files.lines(Paths.get(in_json_file_name))) {

                    String nsDC      = "http://purl.org/dc/terms/";
                    String nsDCType  = "http://purl.org/dc/dcmitype/";
                    String nsRDF     = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
                    String nsRDFs    = "http://www.w3.org/2000/01/rdf-schema#";
                    String nsSIOC    = "http://rdfs.org/sioc/ns#";
                    String nsSIOC_t  = "http://rdfs.org/sioc/types#";

                    Model model = ModelFactory.createDefaultModel();

                    model.setNsPrefix("dc", nsDC);
                    model.setNsPrefix("dctype", nsDCType);
                    model.setNsPrefix("rdf", nsRDF);
                    model.setNsPrefix("rdfs", nsRDFs);
                    model.setNsPrefix("sioc", nsSIOC);
                    model.setNsPrefix("sioc_t", nsSIOC_t);

                    lines.forEach((item) -> {
                        try {
                            JSONObject obj_json = (JSONObject) jsonParser.parse(item);

                            //atributos básicos
                            String id_tweet  = (String) obj_json.get("id_str");
                            String acc_name  = (String) ((JSONObject)obj_json.get("user")).get("screen_name");
                            String label_name  = (String) ((JSONObject)obj_json.get("user")).get("name");
                            String create_at = (String) obj_json.get("created_at");
                            String content   = (String) obj_json.get("text");

                            Resource subject = model.createResource(twitter_url+acc_name+"/status/"+id_tweet);

                            subject.addProperty(model.createProperty(nsDC+"created"), create_at)
                                    .addProperty(model.createProperty(nsSIOC+"has_container"), acc_name)
                                    .addProperty(model.createProperty(nsSIOC+"content"),  content); 

                             Property has_creator = model.createProperty(nsSIOC+"has_creator"); 

                            //caso seja um retweet, o criador do recurso é considerado quem twitou primeiro
                            if (obj_json.containsKey("retweeted_status")) {

                                JSONObject retweeted_obj = (JSONObject) obj_json.get("retweeted_status");

                                if (retweeted_obj.containsKey("user")) {                                
                                    String acc_name_rt = (String)((JSONObject)retweeted_obj.get("user")).get("screen_name");
                                    String rt_label_name = (String)((JSONObject)retweeted_obj.get("user")).get("name");

                                    subject.addProperty(has_creator,  model.createResource()
                                                                        .addProperty(model.createProperty(nsSIOC+"UserAccount"), twitter_url+acc_name_rt)
                                                                        .addProperty(model.createProperty(nsRDFs+"label"), rt_label_name)                                        
                                                        );
                                }

                            }
                            else {

                                subject.addProperty(has_creator,  model.createResource()
                                                                    .addProperty(model.createProperty(nsSIOC+"UserAccount"), twitter_url+acc_name)
                                                                    .addProperty(model.createProperty(nsRDFs+"label"), label_name)
                                                    );                          

                            }
                            
                            //lidando com as hashtags
                            if (obj_json.containsKey("entities") && ((JSONObject) obj_json.get("entities")).containsKey("hashtags")) {
                                JSONArray arr_hashtags = (JSONArray) ((JSONObject) obj_json.get("entities")).get("hashtags");
                                if (arr_hashtags.size() > 0) {

                                    Property sioc_topic = model.createProperty(nsSIOC+"topic");

                                    RDFNode[] elements = new RDFNode[arr_hashtags.size()];

                                    for (int i=0;i<arr_hashtags.size();i++) {
                                        String hash_tag = (String) ((JSONObject)arr_hashtags.get(i)).get("text");
                                        //elements[i] = model.createProperty(nsSIOC_t+"tag", hash_tag);  
                                        elements[i] = model.createLiteral(hash_tag.toLowerCase());
                                    }

                                    subject.addProperty(sioc_topic, model.createList(elements));
                                }
                            }
                            
                            //lidando com as mídias
                            if (obj_json.containsKey("entities") && ((JSONObject) obj_json.get("entities")).containsKey("media")) {

                                JSONArray arr_medias = (JSONArray) ((JSONObject) obj_json.get("entities")).get("media");

                                if (arr_medias.size() > 0) {   


                                    RDFNode[] elements_media = new RDFNode[arr_medias.size()];

                                    //Resource blank_node_media = model.createResource();

                                    Property dc_collection = model.createProperty(nsDC+"hasPart");

                                    for (int i=0;i<arr_medias.size();i++) {

                                        String media_url =  (String) ((JSONObject)arr_medias.get(i)).get("media_url");
                                        String media_type = (String) ((JSONObject)arr_medias.get(i)).get("type");

                                        elements_media[i] = model.createProperty(media_url);   
                                        //elements_media[i] = model.createResource(media_url).addProperty(model.createProperty(nsDC+"type"), nsDCType+media_type);
                                        //elements_media[i] = model.createResource(media_url).addProperty(model.createProperty(nsDC+"type"), nsDCType+media_type);
                                        //blank_node_media.addProperty(model.createProperty(media_url), nsDCType+media_type);
                                        //blank_node_media.addProperty(has_creator, item, item);

                                        Resource subject_media = model.createResource(media_url);
                                        Property predicate_media = model.createProperty(nsDC+"type");
                                        RDFNode object_media = model.createResource(nsDCType+media_type);

                                        subject_media
                                                    .addProperty(model.createProperty(nsDC+"dcterms:creator"), "")
                                                    .addProperty(model.createProperty(nsDC+"dcterms:created"), "");

                                        Statement stmt_media =  model.createLiteralStatement(subject_media,predicate_media,object_media);

                                        subject.getModel().add(stmt_media);

                                    }                                

                                   subject.addProperty(dc_collection, model.createList(elements_media));
                                }
                            }                        


                            Property predicate = model.createProperty(nsRDF+"type");

                            RDFNode object = model.createResource(nsSIOC_t+"MicroblogPost");

                            Statement stmt = model.createStatement(subject, predicate, object);

                            model.add(stmt);
                            
                            

                        } catch (ParseException ex) {
                            System.out.println(ex.getMessage());
                        }
                    });

                    
                    //File file = new File("tweets.ttl");
                    File file = new File(out_ttl_file_name);
                    FileOutputStream fop = new FileOutputStream(file);

                    RDFDataMgr.write(fop, model, RDFFormat.TURTLE);

            } catch (IOException ex) {
                    System.out.println(ex.getMessage());
            }

        }

    }    
}