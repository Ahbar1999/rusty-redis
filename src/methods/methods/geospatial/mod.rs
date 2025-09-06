pub mod geospatial {
    use std::{collections::HashMap, sync::Arc};

    use tokio::sync::Mutex;

    use crate::utils::utils::*;

    pub async fn cmd_geoadd(
        _: &Args,
        cmd_args: &Vec<String>,
        sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>) -> String {
        
        // args format: [_, key, long, lat, member]
        // let GEO_SET_NAME = String::from("GEO"); // all the geolocation entries belong to the GEO set 
        let key = &cmd_args[1]; // set name

        let value = GEOlocation{
            member: cmd_args[4].clone(),
            lat: SortableF64(cmd_args[3].clone().parse().unwrap()),  
            long: SortableF64(cmd_args[2].clone().parse().unwrap()),
        };

        if value.long.0 > 180.0 
            || value.long.0 < -180.0 
            || value.lat.0 < -85.05112878
            || value.lat.0 > 85.05112878 {
            return redis_err(&format!("{} {:?},{:?}", _ERROR_OUT_OF_RANGE_GEOCOORDS_, value.long, value.lat));
        } 

        let mut sorted_set = sorted_set_ref.lock().await;
        
        let set = sorted_set.entry(key.clone()).or_default();
        // let serialized_value = &serde_json::to_string(&value).unwrap();

        // for now 
        // dont store coords
        // score is hardcoded to 0.0
        encode_int(set.insert(&value.member, &SortableF64(geo_encode(value.lat.0, value.long.0) as f64), &value.member))
    }

    pub async fn cmd_geopos(
        cmd_args: &Vec<String>,
        sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>) -> String {

        let mut result = vec![];
        let set_name = &cmd_args[1];

        for i in 2..cmd_args.len() {

            let place = &cmd_args[i];

            if let Some(set) = sorted_set_ref.lock().await.get(set_name) {
                if let Some(score) = set.kv.get(place) {
                    let coords = geo_decode(score.0 as u64);
                    result.push(encode_array(&vec![coords.longitude.to_string(), coords.latitude.to_string()], true));
                } else {
                    result.push("*-1\r\n".to_owned());
                    // result.push(encode_array(&vec![], false));
                }
            } else {

                result.push("*-1\r\n".to_owned());
                // result.push(encode_array(&vec![], false));
            }
        }

        if result.is_empty() {
            return "*-1\r\n".to_owned();
        }

        encode_array(&result, false)
    }

    pub async fn cmd_geodist(
        cmd_args: &Vec<String>,
        sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>,
    ) -> String {

        let set_name = &cmd_args[1]; 
        let key1 = &cmd_args[2];
        let key2 = &cmd_args[3];
        let mut result = encode_bulk("");

        if let Some(set) = sorted_set_ref.lock().await.get(set_name) {
            let coords1 = geo_decode(set.kv.get(key1).unwrap().0 as u64);
            let coords2 = geo_decode(set.kv.get(key2).unwrap().0 as u64);

            result= encode_bulk(haversine_dist(&coords1, &coords2).to_string().as_str());
        }  

        result
    } 
    
    pub async fn cmd_geosearch(
        cmd_args: &Vec<String>,
        sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>,
    ) -> String {
        // 0 1 2 3 4 5 6 7
        // FROMLONLAT, BYRADIUS options are fixed
        // [GEOSEARCH places FROMLONLAT long lat BYRADIUS x m]
        let set_name    =  &cmd_args[1];
        let center  = Coordinates{
            longitude: cmd_args[3].parse().unwrap(), 
            latitude: cmd_args[4].parse().unwrap()
        };  
        let radius: f64;

        match cmd_args[7].as_str() {
            "km" => {
                radius =  cmd_args[6].parse::<f64>().unwrap() * 1000.0;
            },
            "m" => {
                radius =  cmd_args[6].parse::<f64>().unwrap();
            },
            "mi" => {
                radius =  cmd_args[6].parse::<f64>().unwrap() * 1609.34;
            },
            _ => {
                panic!("unidentified distance metric in cmd: geosearch.");
            }
        }

        let mut result = vec![];

        if let Some(set) = sorted_set_ref.lock().await.get(set_name) {
            for (loc, score) in set.kv.iter() {
                if haversine_dist(&center, &geo_decode(score.0 as u64)) <= radius {
                    result.push(loc.clone());
                }
            } 
        }  

        encode_array(&result, true)
    } 

}