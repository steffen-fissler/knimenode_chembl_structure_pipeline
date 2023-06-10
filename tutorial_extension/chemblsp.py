import logging
import knime.extension as knext
from rdkit import Chem
from rdkit.Chem import rdMolHash
from functools import partial
from chembl_structure_pipeline import standardize_mol
from chembl_structure_pipeline import get_parent_mol

LOGGER = logging.getLogger(__name__)
#molhash = partial(rdMolHash,MolHash(rdMolHash.HashFunction.HeAtomTautomer))

@knext.node(name="chembl structure pipeline", node_type=knext.NodeType.MANIPULATOR, icon_path="demo.png", category="/")
@knext.input_table(name="SMILES column", description="read smiles")
@knext.output_table(name="Output Data", description="rdkit mol which is standarized with chembl structure pipeline")
class TemplateNode:
    """Short one-line description of the node.
    This is sample node which is implemented with chembl structure pipeline.
    input data should be SMILES.
    """

    # simple code
    def std_mol(self, smiles):
        mol = Chem.MolFromSmiles(smiles)
        if mol == None:
            return None
        else:
            stdmol = standardize_mol(mol)
            pm, _ = get_parent_mol(stdmol)
            Chem.Kekulize(pm)
            return pm
    
    def get_mol_hash(sel, rdmol):
        res = rdMolHash.MolHash(rdmol, rdMolHash.HashFunction.HetAtomTautomer)
        return res

    column_param = knext.ColumnParameter(label="label", description="description", port_index=0)
   

    def configure(self, configure_context, input_schema_1):   
            
        #return input_schema_1.append(knext.Column(Chem.rdchem.Mol, "STD_ROMol"))
        return input_schema_1.append(knext.Column(Chem.rdchem.Mol, "STD_ROMol")).append(knext.Column(knext.string(), 'MolHash'))

 
    def execute(self, exec_context, input_1):
        input_1_pandas = input_1.to_pandas()
        input_1_pandas['STD_ROMol'] = input_1_pandas['column1'].apply(self.std_mol)
        input_1_pandas['MolHash'] = input_1_pandas['STD_ROMol'].apply(self.get_mol_hash)
        return knext.Table.from_pandas(input_1_pandas)

