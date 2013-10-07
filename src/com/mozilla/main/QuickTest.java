package com.mozilla.main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class QuickTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
        int minimum = 1;
        int maximum = 1000;
        Random rn = new Random();
        int range = maximum - minimum + 1;
        int randomNum =  rn.nextInt(range) + minimum;
        System.out.println(randomNum);
        randomNum =  rn.nextInt(range) + minimum;
        System.out.println(randomNum);
        System.exit(1);
        ObjectMapper mapper = new ObjectMapper(); // can reuse, share globally
        BufferedReader br = null;

        try {

            String sCurrentLine;

            br = new BufferedReader(new FileReader("json/1.json"));
            boolean validFHRDocument = false;

            while ((sCurrentLine = br.readLine()) != null) {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                validFHRDocument = false;
                JsonNode rootNode = mapper.readValue(sCurrentLine, JsonNode.class); // src can be a File, URL, InputStream etc
                int fhrVersion = rootNode.path("version").getIntValue();
                Date thisPingDate = null, lastPingDate = null;
                String updateChannel = null, os = null, country = null;
                int profileCreation = -1, memoryMB = -1;

                if (fhrVersion == 2) {
                    validFHRDocument = true;
                    System.out.println("valid fhr version");
                    try {
                        thisPingDate = formatter.parse(rootNode.path("thisPingDate").getTextValue());
                    } catch (ParseException e) {
                        // TODO Auto-generated catch block
                        validFHRDocument = false;
                        e.printStackTrace();
                    }
                    try {
                        lastPingDate = formatter.parse(rootNode.path("lastPingDate").getTextValue());
                    } catch (ParseException e) {
                        // TODO Auto-generated catch block
                        validFHRDocument = false;
                        e.printStackTrace();
                    }

                    updateChannel = rootNode.path("geckoAppInfo").path("updateChannel").getTextValue().trim();
                    if (StringUtils.isBlank(updateChannel)) {
                        updateChannel = rootNode.path("data").path("last").path("org.mozilla.appInfo.appinfo").path("updateChannel").getTextValue().trim();
                        if (StringUtils.isBlank(updateChannel)) {    
                            validFHRDocument = false;
                        }
                    }

                    os = rootNode.path("geckoAppInfo").path("os").getTextValue().trim();
                    if (StringUtils.isBlank(os)) {
                        os = rootNode.path("data").path("last").path("org.mozilla.appInfo.appinfo").path("os").getTextValue().trim();
                        if (StringUtils.isBlank(os)) {    
                            validFHRDocument = false;
                        }
                    }

                    profileCreation = rootNode.path("data").path("last").path("org.mozilla.profile.age").path("profileCreation").getIntValue();
                    if (profileCreation == -1) {    
                        validFHRDocument = false;
                    }

                    country = rootNode.path("geoCountry").getTextValue();
                    if (StringUtils.isBlank(country)) {    
                        validFHRDocument = false;
                    }

                    memoryMB = rootNode.path("data").path("last").path("org.mozilla.sysinfo.sysinfo").path("memoryMB").getIntValue();
                    if (memoryMB == -1) {    
                        validFHRDocument = false;
                    }


                    Iterator<String> s = rootNode.path("data").path("days").getFieldNames();
                    List<Date> dataDays = new ArrayList<Date>();

                    while(s.hasNext()){
                        try {
                            String tempDay = s.next();
                            dataDays.add(formatter.parse(tempDay));
                        } catch (ParseException e) {
                            // TODO Auto-generated catch block
                            validFHRDocument = false;
                            e.printStackTrace();
                        }

                    }
                    int numAppSessionsPreviousOnThisPingDate = 0;
                    numAppSessionsPreviousOnThisPingDate = rootNode.path("data").path("days").path(formatter.format(thisPingDate)).path("org.mozilla.appSessions.previous").path("main").size();

                    int currentSessionTime = 0;
                    currentSessionTime = rootNode.path("data").path("last").path("org.mozilla.appSessions.current").path("totalTime").getIntValue();

                    List<Date> activeDays = new ArrayList<Date>();
                    activeDays.add(thisPingDate);
                    if (dataDays != null) {
                        activeDays.addAll(dataDays);
                    }
                    if (lastPingDate != null) {
                        activeDays.add(lastPingDate);
                    }
                    Collections.sort(activeDays);
                    Date firstValidFHRDate = null;

                    boolean firstValidFHRDateFound = false;
                    if (dataDays.size() > 0) {
                        for (Date d : activeDays) {
                            if (firstValidFHRDateFound) {
                                break;
                            }
                            System.out.println("D: " + formatter.format(d));
                            Iterator <String> dateIterator = rootNode.path("data").path("days").path(formatter.format(d)).getFieldNames();
                            while (dateIterator.hasNext()) {
                                String tempDay = dateIterator.next();
                                if (!(StringUtils.equals(tempDay, "org.mozilla.crashes.crashes") || StringUtils.equals(tempDay, "org.mozilla.appSessions.previous"))) {
                                    System.out.println("First Valid FHR date: " + formatter.format(d));
                                    firstValidFHRDate = d;
                                    firstValidFHRDateFound = true;
                                    break;
                                }
                            }
                        }
                        
                        if (!firstValidFHRDateFound) {
                            firstValidFHRDate = activeDays.get(0);
                            firstValidFHRDateFound = true;
                        }
                    }
                    
                    if (!firstValidFHRDateFound) {
                        firstValidFHRDate = activeDays.get(0);
                        firstValidFHRDateFound = true;
                    }

                    String firstFhrActiveDayData = null;
                    for (Date d : activeDays) {
                        if (d.equals(firstValidFHRDate) || d.after(firstValidFHRDate)) {
                            firstFhrActiveDayData = rootNode.path("data").path("days").path(formatter.format(d)).toString();
                            
                            break;
                        }
                    }
                    

                } else {
                    validFHRDocument = false;
                    System.err.println("invalid fhr version json");
                }
            }
            if (validFHRDocument) {
                System.out.println("valid document");
            } else {
                System.err.println("invalid fhr document");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }        
        System.out.println("hello FHR");


    }

}
