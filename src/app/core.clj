(ns office-worker.core
  (:require [clojure.java.io :refer [resource]])
  (:import (com.google.api.client.googleapis.auth.oauth2 GoogleCredential
                                                         GoogleCredential$Builder)
           (com.google.api.client.http HttpTransport)
           (com.google.api.client.http.javanet NetHttpTransport)
           (com.google.api.client.json JsonFactory)
           (com.google.api.client.json.jackson2 JacksonFactory)
           (com.google.gdata.client GoogleService)
           (com.google.gdata.client.spreadsheet SpreadsheetService)
           (com.google.gdata.data IEntry ILink$Rel ILink$Type)
           (com.google.gdata.data.batch BatchOperationType BatchUtils)
           (com.google.gdata.data.spreadsheet Cell CellEntry CellFeed
                                              ListEntry ListFeed
                                              SpreadsheetEntry
                                              SpreadsheetFeed
                                              WorksheetEntry
                                              WorksheetFeed)
           (java.io File)
           (java.net URL)))

(defn filter-map [pred m]
  (select-keys m (for [[k v] m :when (pred v)] k)))

(defn ^HttpTransport http-transport
  []
  (NetHttpTransport.))

(defn ^JsonFactory json-factory
  []
  (JacksonFactory.))

(defn spreadsheet-scope []
  "https://spreadsheets.google.com/feeds")

(defn ^GoogleCredential service-account-credential
  [{:keys [account-id key-path]}]
  (.build
   (doto (GoogleCredential$Builder. )
     (.setTransport (http-transport))
     (.setJsonFactory (json-factory))
     (.setServiceAccountId account-id)
     (.setServiceAccountScopes [(spreadsheet-scope)])
     (.setServiceAccountPrivateKeyFromP12File (File.
                                               (.toURI (resource key-path)))))))

(defn spreadsheet-service [^GoogleCredential credential]
  (doto (SpreadsheetService. "Nibbol")
    (.setOAuth2Credentials credential)))

(defn feed [^GoogleService service ^URL feed-url ^Class class]
  (.getFeed service feed-url class))

(defn worksheet-feed-url [^SpreadsheetEntry spreadsheet]
  (.getWorksheetFeedUrl spreadsheet))

(defn cell-feed-url [^WorksheetEntry entry]
  (.getCellFeedUrl entry))

(defn spreadsheets [^SpreadsheetService service]
  (let [feed-url (URL. "https://spreadsheets.google.com/feeds/spreadsheets/private/full")]
    (-> (feed service feed-url SpreadsheetFeed)
        .getEntries)))

(defn worksheets
  [^SpreadsheetService service
   ^SpreadsheetEntry spreadsheet]
  (-> (feed service (worksheet-feed-url spreadsheet) WorksheetFeed)
      .getEntries))

(defn find-entries [entries name]
  (filter #(= name (-> % .getTitle .getPlainText)) entries))

(defn worksheet-entry [& [title rows cols]]
  (let [ws (WorksheetEntry.)]
    (when title
      (.setTitle ws title))
    (when cols
      (.setColCount ws cols))
    (when rows
      (.setRowCount ws rows))
    ws))

(defn insert-entry
  [^SpreadsheetService service
   ^URL feed
   ^IEntry entry]
  (.insert service feed entry)
  service)

(defn cell-id [row coll]
  (format "R%sC%s" row coll))

(defn cell-feed->vectors
  [^CellFeed feed]
  (into {} (map (fn [entry]
                  (let [cell (.getCell entry)]
                    [[(.getRow cell) (.getCol cell)]
                     (.getValue cell)]))
                (.getEntries feed))))

(defn vectors->cell-feed
  [^URL feed-url
   entries]
  (let [cell-entries (mapv (fn [[[r c] v]]
                             (doto (CellEntry. (Cell. r c v))
                               (.setId (format "%s/%s"
                                               (.toString feed-url)
                                               (cell-id r c)))))
                           entries)]
    (doto (CellFeed.)
      (.setEntries cell-entries))))

(defn cells
  [^SpreadsheetService service
   ^WorksheetEntry worksheet]
  (let [cell-feed (feed service (cell-feed-url worksheet) CellFeed)]
    (cell-feed->vectors cell-feed)))

(defn update-cells
  [^SpreadsheetService service
   ^WorksheetEntry worksheet
   cells]
  (let [cell-url (cell-feed-url worksheet)
        batch-request (vectors->cell-feed cell-url cells)
        feed (feed service cell-url CellFeed)
        feed-batch-url (-> feed
                           (.getLink ILink$Rel/FEED_BATCH
                                     ILink$Type/ATOM)
                           .getHref
                           (URL.))]
    (mapv #(BatchUtils/setBatchOperationType % BatchOperationType/UPDATE)
          (.getEntries batch-request))
    (let [_ (.setHeader service "If-Match" "*")
          batch-response (.batch service feed-batch-url batch-request)
          _ (.setHeader service "If-Match" nil)]
      batch-response)))

(defn rows
  [^SpreadsheetService service
   ^WorksheetEntry worksheet]
  (let [cell-rows (->> (feed service (cell-feed-url worksheet) CellFeed)
                       .getEntries
                       (group-by #(.. % getCell getRow))
                       vals)
        header (map #(.. % getCell getValue) (first cell-rows))
        row-maps (map (fn [row]
                        (into {}
                              (map (fn [cell]
                                     [(nth header (dec (.. cell getCell getCol)))
                                      (.. cell getCell getValue)])
                                   row)))
                      (rest cell-rows))]
    row-maps))


(defn set-worksheet-headers
  [^SpreadsheetService service
   ^WorksheetEntry worksheet
   headers]
  (let [cell-url (cell-feed-url worksheet)
        entries (map (fn [index]
                       (let [id (cell-id 1 (inc index))
                             entry (CellEntry. 1 (inc index) "")
                             entry-id (format "%s/%s"
                                              (.toString cell-url)
                                              id)]
                         (doto entry
                           (.setId entry-id)
                           (BatchUtils/setBatchOperationType
                            BatchOperationType/QUERY))))
                     (range (count headers)))
        batch-request (doto (CellFeed.)
                        (.setEntries entries))
        ws-cell-feed (feed service cell-url CellFeed)
        feed-batch-url (-> ws-cell-feed
                           (.getLink ILink$Rel/FEED_BATCH
                                     ILink$Type/ATOM)
                           .getHref
                           (URL.))
        batch-response (.batch service
                               feed-batch-url
                               batch-request)
        entries (map (fn [entry header]
                       (doto (CellEntry.
                              (.withNewInputValue (.getCell entry) (str header)))
                         (.setId (.getId entry))
                         (BatchUtils/setBatchOperationType
                          BatchOperationType/UPDATE)))
                     (.getEntries batch-response)
                     headers)
        batch-request (doto (CellFeed.)
                        (.setEntries entries))
        _ (.setHeader service "If-Match" "*")
        batch-response (.batch service
                               feed-batch-url 
                               batch-request)
        _ (.setHeader service "If-Match" nil)]
    batch-response))

(defn set-row-value [^ListEntry row key value]
  (-> (.getCustomElements row)
      (.setValueLocal key value)))

(defn update-rows
  [^SpreadsheetService service
   ^WorksheetEntry worksheet
   values]
  (let [list-feed (feed service
                        (.getListFeedUrl worksheet)
                        ListFeed)]
    (mapv (fn [value]
            (let [entry (ListEntry.)]
              (mapv (fn [[k v]]
                      (set-row-value entry k v))
                    value)
              (.insert service (.getListFeedUrl worksheet) entry))) 
          values)))

(defn live-neighbors [cells r c]
  (for [rs (filter pos? (range (dec r) (+ r 2)))
        cs (filter pos? (range (dec c) (+ c 2)))
        :when (and (not (and (= rs r)
                             (= cs c)))
                   (= "1" (get cells [rs cs])))]
    [rs cs]))

(defn under-pop? [cells [[r c] live]]
  (when (and live (> 2 (count (live-neighbors cells r c))))
    true))

(defn over-crowd? [cells [[r c] live]]
  (when (and live (< 3 (count (live-neighbors cells r c))))
    true))

(defn repro? [cells [[r c] live]]
  (when (and (not= "1" live) (= 3 (count (live-neighbors cells r c))))
    true))

(defn conway-rules [cells cell]
  (let [[[r c] live] cell]
    [[r c]
     (cond
      (under-pop? cells cell) ""
      (over-crowd? cells cell) ""
      (repro? cells cell) "1"
      (= live "1") "1"
      :else "")]))

(defn calc-cells [cells]
  (into {} (mapv (fn [cell-data]
                   (conway-rules cells cell-data))
                 cells)))

(defn -main [& args]
  (let [credential (service-account-credential
                    {:account-id ""
                     :key-path ""})
        ss (spreadsheet-service credential)
        spreadsheet (first (find-entries (spreadsheets ss) "conway"))
        worksheet (first (worksheets ss spreadsheet))
        cell-feed (feed ss (cell-feed-url worksheet) CellFeed)]
    (loop [cell-vectors (cell-feed->vectors cell-feed)]
      (let [cell-vectors (filter-map #(= "1" %) cell-vectors)
            cell-vectors-0 (reduce (fn [cells [[r c] val]]
                                     (reduce (fn [cells rc]
                                               (update-in cells [rc]
                                                          #(if (= "1" %)
                                                             %
                                                             0)))
                                             cells
                                             (for [rs (filter pos? (range (dec r) (+ r 2)))
                                                   cs (filter pos? (range (dec c) (+ c 2)))]
                                               [rs cs])))
                                   cell-vectors
                                   cell-vectors)
            calced-cells (calc-cells cell-vectors-0)]
        (update-cells ss worksheet calced-cells)
        (Thread/sleep 100)
        (recur calced-cells)))))



