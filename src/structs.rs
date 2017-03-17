use bt::support::{Project, Instance, Table};
use bt::utils::*;
use fdw_error::Result;
use goauth::auth::Token;
use goauth::credentials::Credentials;
use libc::c_int;
use pg;
use rustc_serialize::base64::FromBase64;
use serde_json;
use std::ffi::{CString, CStr};

#[derive(Debug)]
pub struct BtFdwState {
    pub token: Result<Token>,
    pub user: FdwUser,
    pub has_data: bool,
    pub data: FdwSelectData
}

impl BtFdwState {
    fn project(&self) -> Result<Project> {
        Ok(Project {
            name: match get_option("project", &self.user.server.options.as_slice()) {
                Some(x) => x.value,
                None => bail!("Undefined project ID")
            }
        })
    }

    fn instance(&self) -> Result<Instance> {
        Ok(Instance {
            name: match get_option("instance", &self.user.server.options.as_slice()) {
                Some(x) => x.value,
                None => bail!("Undefined instance ID")
            },
            project: self.project()?
        })
    }

    pub fn table(&self) -> Result<Table> {
        Ok(Table {
            name: match get_option("name", &self.user.server.table.options.as_slice()) {
                Some(x) => x.value,
                None => bail!("Undefined table name")
            },
            instance: self.instance()?
        })
    }
}

#[derive(Debug)]
pub struct FdwUser {
    id: pg::Oid,
    server: FdwServer,
    credentials: Option<Credentials>,
    options: Vec<FdwOpt>,
    valid_options: Vec<String>
}

impl FdwUser {
    pub fn from(srv: FdwServer, curruser: pg::Oid) -> Self {
        let valid_options = vec!(String::from("credentials_path"));
        let fuser = unsafe {
            let fu = pg::GetUserMapping(curruser, srv.id);
            assert!(!fu.is_null());
            *fu
        };
        let opts = extract_options(fuser.options, Some(&valid_options.as_slice()));
        let creds = Credentials::from_file(&get_option("credentials_path", &opts.as_slice()).unwrap().value).unwrap();
        FdwUser {
            id: fuser.userid,
            server: srv,
            options: opts,
            valid_options: valid_options,
            credentials: Some(creds)
        }
    }

    pub fn authenticate(&self) -> Result<Token> {
        Ok(get_auth_token(match self.options.first() {
            Some(ref x) => &x.value,
            None => bail!("No user mapping defined it seems...")
        }, true)?)
    }
}

#[derive(Debug)]
pub struct FdwTable {
    id: pg::Oid,
    options: Vec<FdwOpt>,
    valid_options: Vec<String>,
    relation: Relation,
    server_id: pg::Oid
}

impl From<Relation> for FdwTable {
    fn from(rel: Relation) -> Self {
        let valid_options = vec!(String::from("name"));
        let ftable: pg::ForeignTable = unsafe {
            let ft = pg::GetForeignTable(rel.id);
            assert!(!ft.is_null());
            *ft
        };
        FdwTable {
            id: ftable.relid,
            options: extract_options(ftable.options, Some(&valid_options)),
            valid_options: valid_options,
            relation: rel,
            server_id:
            ftable.serverid
        }
    }
}

#[derive(Debug)]
pub struct FdwServer {
    id: pg::Oid,
    options: Vec<FdwOpt>,
    valid_options: Vec<String>,
    table: FdwTable,
}

impl From<FdwTable> for FdwServer {
    fn from(table: FdwTable) -> Self {
        let valid_options = vec!(String::from("instance"), String::from("project"));
        let fserver: pg::ForeignServer = unsafe {
            let fs = pg::GetForeignServer(table.server_id);
            assert!(!fs.is_null());
            *fs
        };
        FdwServer {
            id: fserver.serverid,
            options: extract_options(fserver.options, Some(&valid_options)),
            valid_options: valid_options,
            table: table
        }
    }
}

fn get_option(target: &str, options: &[FdwOpt]) -> Option<FdwOpt> {
    for option in options {
        if &option.name == target {
            return Some(option.clone())
        }
    }
    None
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct FdwRow {
    rowKey: String,
    familyName: String,
    qualifier: String,
    value: String,
    commitRow: bool
}

impl FdwRow {
    pub fn from(json: serde_json::Value) -> Result<FdwRow> {
        let row: FdwRow = serde_json::from_value(json)?;
        Ok(
            FdwRow {
                rowKey: String::from_utf8(row.rowKey.as_bytes().from_base64()?)?,
                familyName: row.familyName,
                qualifier: String::from_utf8(row.qualifier.as_bytes().from_base64()?)?,
                value: String::from_utf8(row.value.as_bytes().from_base64()?)?,
                commitRow: row.commitRow
            }
        )
    }
}

#[derive(Debug, Clone)]
pub struct FdwOpt {
    name: String,
    value: String
}

impl FdwOpt {
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl From<*mut pg::DefElem> for FdwOpt {
    fn from(def: *mut pg::DefElem) -> Self {
        let defname;
        let str_val;
        unsafe {
            assert!(!def.is_null());
            let def = *def;
            defname = CStr::from_ptr(def.defname);
            let val = def.arg as *mut pg::Value;
            assert!(!val.is_null());
            let str_ptr = *(*val).val.str.as_mut();
            assert!(!str_ptr.is_null());
            str_val = CStr::from_ptr(str_ptr);
        }
        FdwOpt {
            name: defname.to_str().unwrap().to_string(),
            value: str_val.to_str().unwrap().to_string()
        }
    }
}

#[derive(Debug)]
pub struct Relation {
    id: pg::Oid,
    pg_rel: pg::RelationData,
    rel_rd_att: pg::tupleDesc
}

impl From<*mut pg::RelationData> for Relation {
    fn from(rd: *mut pg::RelationData) -> Self {
        let rd = unsafe {
            assert!(!rd.is_null());
            *rd
        };
        let att = unsafe { *rd.rd_att };
        Relation {
            id: rd.rd_id,
            pg_rel: rd,
            rel_rd_att: att
        }
    }
}

#[derive(Debug)]
pub struct Node {
    pub relation: Relation,
    options: Vec<FdwOpt>,
    pub slot: Option<*mut pg::TupleTableSlot>
}

impl Node {
    pub fn assign_slot(&mut self, v: String) {
        unsafe {
            let t = CString::from_vec_unchecked(v.into_bytes());
            let attinmeta = pg::TupleDescGetAttInMetadata(&mut self.relation.rel_rd_att);
            let tuple = pg::BuildTupleFromCStrings(attinmeta, &mut t.into_raw());
            pg::ExecStoreTuple(tuple, self.slot.expect("No slot avaliable, this should not happen, Node likely spawned from wrong type, check From"), 0, 0);
        }
    }
}

impl From<*mut pg::ForeignScanState> for Node {
    fn from(fss: *mut pg::ForeignScanState) -> Self {
        let relation = unsafe {
            assert!(!fss.is_null());
            Relation::from((*fss).ss.ss_currentRelation)
        };
        let slot = unsafe {
            (*fss).ss.ss_ScanTupleSlot
        };
        let opts = unsafe {
            let opt_list = (*fss).fdw_recheck_quals;
            extract_options(opt_list, None)
        };
        Node {
            relation: relation,
            options: opts,
            slot: Some(slot)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FdwError {
    code: i32,
    message: String,
    status: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FdwErrorObj {
    error: Vec<FdwError>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FdwSelectData {
    pub chunks: Vec<serde_json::Value>
}

impl FdwSelectData {
    pub fn from(json: serde_json::Value) -> Result<FdwSelectData> {
        let mut r: Vec<FdwSelectData> = serde_json::from_value(json)?;
        let mut joint = FdwSelectData { chunks: Vec::new() };
        for mut d in r.drain(..) {
            joint.chunks.append(&mut d.chunks)
        }
        Ok(joint)
    }
}

impl From<*mut pg::ResultRelInfo> for Node {
    fn from(rri: *mut pg::ResultRelInfo) -> Self {
        let relation = unsafe {
            assert!(!rri.is_null());
            Relation::from((*rri).ri_RelationDesc)
        };
        Node {
            relation: relation,
            options: Vec::new(),
            slot: None
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FdwInsData {
    pub data: Vec<serde_json::Value>
}


fn extract_options(opts: *mut pg::List, opts_to_get: Option<&[String]>) -> Vec<FdwOpt> {
    if opts.is_null() {
        return Vec::new()
    }
    let mut out_opts = Vec::new();
    let ls = unsafe {
        assert!(!opts.is_null());
        *opts
    };
    let l = ls.length;
    for idx in 0..l {
        let def = cell_to_def(&ls, idx);
        let opt = FdwOpt::from(def);
        match opts_to_get {
            Some(opts) => {
                match get_opt_if_allowed(opts, opt) {
                    Some(o) => out_opts.push(o),
                    None => {}
                }
            },
            None => out_opts.push(opt)
        }
    }
    out_opts
}

fn cell_to_def(ls: &pg::List, idx: c_int) -> *mut pg::DefElem {
    unsafe {
        let cell_ptr = pg::list_nth_cell(ls, idx);
        assert!(!cell_ptr.is_null());
        let cell = (*cell_ptr).data.ptr_value.as_mut();
        *cell as *mut pg::DefElem
    }
}

fn get_opt_if_allowed(opts_to_get: &[String], opt: FdwOpt) -> Option<FdwOpt> {
    for opt_to_get in opts_to_get {
        if opt_to_get == opt.name() {
            return Some(opt);
        }
    }
    return None
}