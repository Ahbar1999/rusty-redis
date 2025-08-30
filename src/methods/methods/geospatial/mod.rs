pub mod geospatial {
    use std::{collections::HashMap, sync::Arc};

    use tokio::sync::Mutex;

    use crate::utils::utils::*;

    pub async fn cmd_geoadd(
        _: &Args,
        cmd_args: &Vec<String>,
        sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>) -> String {
        
        // args format: [_, key, long, lat, member]
        let GEO_SET_NAME = String::from("GEO"); // all the geolocation entries belong to the GEO set 
        let key = &cmd_args[1];

        let value = GEOlocation{
            member: cmd_args[4].clone(),
            lat: SortableF64(cmd_args[3].clone().parse().unwrap()),  
            long: SortableF64(cmd_args[2].clone().parse().unwrap()),
        };

        if value.long > SortableF64(180.0) 
            || value.long < SortableF64(-180.0) 
            || value.lat < SortableF64(-85.05112878)
            || value.lat > SortableF64(85.05112878) {
            return redis_err(&format!("{} {:?},{:?}", _ERROR_OUT_OF_RANGE_GEOCOORDS_, value.long, value.lat));
        } 

        let mut sorted_set = sorted_set_ref.lock().await;
        
        let set = sorted_set.entry(GEO_SET_NAME.clone()).or_default();

        encode_int(set.insert(key, &SortableF64(0.0), &serde_json::to_string(&value).unwrap()))
    }
}