pub mod sorted_sets {
    use std::{collections::HashMap, sync::Arc};

    use tokio::sync::Mutex;

    use crate::utils::utils::*;

    pub async fn cmd_zadd(
        _: &Args,
        cmd_args: &Vec<String>,
        sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>) -> String {

        let mut sorted_set = sorted_set_ref.lock().await;
        
        let set_name = &cmd_args[1];
        let score: &SortableF64 = &SortableF64{0: cmd_args[2].parse::<f64>().unwrap()};
        let value = &cmd_args[3];

        let set = sorted_set.entry(set_name.clone()).or_default();

        encode_int(set.insert(value, score, value))
    }

    pub async fn cmd_zrange(
        _: &Args,
        cmd_args: &Vec<String>,
        sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>
    ) -> String {
        let set_name = &cmd_args[1];
        // let score = SortableF64(cmd_args[1].parse::<f64>().unwrap());
        let mut start: isize = cmd_args[2].parse::<isize>().unwrap();
        let mut end: isize = cmd_args[3].parse::<isize>().unwrap(); 

        let sorted_set = sorted_set_ref.lock().await;

        let mut result = vec![];
        if let Some(set) = sorted_set.get(set_name) {
            let sz: isize = set.st.len() as isize;
            if start < 0 {
                start = std::cmp::max(0, sz + start);
            } 

            if end < 0 {
                end = std::cmp::min(sz - 1, sz + end);
            }

            let mut i =0;
            for (_, this_key) in set.st.iter() {
                if start <= i && i <= end {
                    result.push(this_key.clone());
                }
                i += 1; 
            }
        }

        encode_array(&result, true)
    }

    pub async fn cmd_zrank(
        _: &Args,
        cmd_args: &Vec<String>,
        sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>
    ) -> String {
        let set_name = &cmd_args[1];
        // let score = SortableF64(cmd_args[1].parse::<f64>().unwrap());
        let key = &cmd_args[2];

        let sorted_set = sorted_set_ref.lock().await;

        let mut rank: isize = -1;
        if let Some(set) = sorted_set.get(set_name) {
            if let Some(score) = set.kv.get(key) {
                for (this_score, this_key) in set.st.iter() {
                    rank += 1;
                    if this_key == key && this_score == score {
                        break;
                    }
                }
            }
        }

        if rank == -1 { 
            return encode_bulk("");
        }
        // else  
        encode_int(rank as usize) 
    }

    pub async fn cmd_zcard(
        _: &Args,
        cmd_args: &Vec<String>,
        sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>) -> String {

        let set_name = &cmd_args[1]; 

        let size;
        if let Some(set) = sorted_set_ref.lock().await.get(set_name) {
            size = set.st.len();
        } else {
            size =0;
        }
    
        encode_int(size) 
    }

    pub async fn cmd_zscore(
        _: &Args,
        cmd_args: &Vec<String>,
        sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>) -> String {
        
        let set_name = &cmd_args[1]; 
        let member = &cmd_args[2];

        if let Some(set) = sorted_set_ref.lock().await.get(set_name) {
            if let Some(score) = set.kv.get(member) {
                return encode_bulk(score.0.to_string().as_str());
            }
        }

        encode_bulk("")  
    }

    pub async fn cmd_zrem(
        _: &Args,
        cmd_args: &Vec<String>,
        sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>) -> String {
            
        let set_name = &cmd_args[1];
        let member = &cmd_args[2];
        
        if let Some(set) = sorted_set_ref.lock().await.get_mut(set_name) {
            if let Some(&score) = set.kv.get(member) {
                set.kv.remove(member.as_str());
                set.st.remove(&(score.clone(), member.clone()));

                return encode_int(1);
            }
        }

        // no elements were deleted because either the set doesnt exist or the member doesnt exist 
        encode_int(0)
    }
}