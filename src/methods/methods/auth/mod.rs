pub mod auth {
    use std::sync::Arc;

    use tokio::sync::Mutex;

    use crate::utils::utils::*;

    fn cmd_list(cmds: &Vec<(usize, Vec<String>)>, 
        // config_args: &mut Args,
        // storage_ref: Arc<Mutex<HashMap<String, (RDBValue, Option<SystemTime>)>>>,
        // sorted_set_ref: Arc<Mutex<HashMap<String, SortedSet>>>,
        // tx: broadcast::Sender<Vec<u8>>,
        glob_config: Arc<Mutex<GlobConfig>>) -> String {

        // return an array of [for each user: bulk string encoded "{username} "on/off" {password}"]; on/off stand for enabled disabled  

        unimplemented!(); 
    }
}