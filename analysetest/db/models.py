from sqlalchemy import Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class PrixHistoriques(Base):
    __tablename__ = 'prix_historiques'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbole = Column(String(20), nullable=False)
    date = Column(Date, nullable=False)
    prix_cloture = Column(Float, nullable=False)
    volume_transactions = Column(Float, nullable=True)
    actions_en_circulation = Column(Float, nullable=True)
    capitalisation_boursiere = Column(Float, nullable=True)

class EtatsFinanciers(Base):
    __tablename__ = 'etats_financiers'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbole = Column(String(20), nullable=False)
    annee = Column(Integer, nullable=False)
    periode = Column(String(20), nullable=False) # 'Annuel', 'Semestriel', 'Trimestriel'
    
    # Compte de Résultat
    chiffre_affaires = Column(Float, nullable=True)
    ebitda = Column(Float, nullable=True)
    resultat_net = Column(Float, nullable=True)
    
    # Bilan Comptable
    capitaux_propres = Column(Float, nullable=True)
    dette_court_terme = Column(Float, nullable=True)
    dette_long_terme = Column(Float, nullable=True)
    tresorerie = Column(Float, nullable=True)
    total_actif = Column(Float, nullable=True)
    
    # Flux de Trésorerie
    free_cash_flow = Column(Float, nullable=True)
    
    # Actionnariat
    dividendes_par_action = Column(Float, nullable=True)

class Macroeconomie(Base):
    __tablename__ = 'macroeconomie'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    taux_directeur = Column(Float, nullable=True)
    taux_inflation = Column(Float, nullable=True)
    rendement_bons_tresor_10a = Column(Float, nullable=True)
