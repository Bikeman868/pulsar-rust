use crate::persistence::{
    entity_persister::{LoadError, SaveError},
    persisted_entities::Cluster,
};
use super::{DataLayer, DataReadError, DataReadResult, DataUpdateError, DataUpdateResult};

impl DataLayer {
    pub fn get_cluster(self: &Self) -> DataReadResult<Cluster> {
        match self.persistence.load(&Cluster::key(&self.cluster_name)) {
            Ok(cluster) => Ok(cluster),
            Err(e) => match e {
                LoadError::Error { msg } => {
                    panic!("{} {}", msg, "getting the cluster record from the database")
                }
                LoadError::NotFound {
                    entity_type: _,
                    entity_key: _,
                } => {
                    let mut cluster =
                        Cluster::new(&self.cluster_name, Vec::new(), Vec::new(), 1, 1);
                    self.persistence
                        .save(&mut cluster)
                        .expect("create cluster record in the database");
                    Ok(cluster)
                }
            },
        }
    }

    pub fn update_cluster<F>(self: &Self, mut update: F) -> DataUpdateResult<Cluster>
    where
        F: FnMut(&mut Cluster) -> bool,
    {
        loop {
            let mut cluster = match self.get_cluster() {
                Ok(cluster) => cluster,
                Err(err) => return match err {
                    DataReadError::PersistenceFailure { msg } => Err(DataUpdateError::PersistenceFailure { msg }),
                    DataReadError::NotFound => Err(DataUpdateError::NotFound),
                }
            };
    
            update(&mut cluster);

            match self.persistence.save(&mut cluster) {
                Ok(_) => return DataUpdateResult::Ok(cluster),
                Err(e) => match e {
                    SaveError::Unmodified => return DataUpdateResult::Ok(cluster),
                    SaveError::VersionMissmatch => continue,
                    SaveError::Error { msg } => {
                        return DataUpdateResult::Err(DataUpdateError::PersistenceFailure {
                            msg: format!("{msg} updating the cluster"),
                        })
                    }
                },
            }
        }
    }
}
