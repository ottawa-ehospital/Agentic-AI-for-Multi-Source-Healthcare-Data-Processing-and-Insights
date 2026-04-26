import 'package:elg6131/localization/app_strings.dart';

const frStrings = AppStrings(
  languageEnglish: 'English',
  languageFrench: 'Français',
  appEyebrow: 'Analytique connectée',
  appTitle: "Espace d'analyse de l'entrepôt",
  appCopy:
      "Connectez des bases sources, chargez les tables sélectionnées dans l'entrepôt et gardez une conversation d'analyse continue sur les données déjà intégrées.",
  connectorTitle: 'Connecteur de base de données',
  connectorSubtitle:
      'Commencez par une connexion à un hôte, puis choisissez une base de données et les tables à charger.',
  sourceTypeLabel: 'Type de source',
  sourceTypeDatabase: 'Base de données',
  sourceTypeTableApi: 'API de table',
  hostPlaceholder: 'Hôte',
  portPlaceholder: 'Port',
  userPlaceholder: 'Utilisateur',
  passwordPlaceholder: 'Mot de passe',
  apiBaseUrlPlaceholder: 'URL de base',
  apiTokenPlaceholder: "Jeton d'API (optionnel)",
  tableApiHelper:
      "Utilisez une source API de table quand la source est exposée en /tables et /table/<nom>. Entrez l'URL de base et, au besoin, un jeton d'API, puis connectez-vous et choisissez les tables.",
  connectorHelper:
      "Les informations de connexion restent dans la session courante et servent uniquement à découvrir les tables et lancer les chargements.",
  connectSource: 'Connecter la source',
  connecting: 'Connexion...',
  connected: 'Connecté',
  disconnect: 'Déconnecter',
  databaseLabel: 'Base de données',
  selectDatabase: 'Sélectionnez une base source',
  loadTablesLabel: 'Charger les tables sources',
  loadTables: 'Charger les tables',
  loadingTables: 'Chargement...',
  selectTablesLabel: 'Sélectionner des tables',
  filterTables: 'Filtrer les tables',
  chooseTableFirst: 'Choisissez au moins une table pour continuer.',
  ingestAndRefresh: 'Ingérer et actualiser les vues',
  loadingWarehouse: "Chargement dans l'entrepôt...",
  loadedSourcesTitle: 'Sources chargées',
  loadedSourcesSubtitle:
      "Inventaire rapide de ce qui a déjà été chargé dans l'entrepôt.",
  noLoadInventory: "Aucun inventaire de chargement n'a encore été trouvé.",
  sourceHost: 'Hôte source',
  refreshAll: 'Tout actualiser',
  refreshing: 'Actualisation...',
  deleteAll: 'Tout supprimer',
  databasePrefix: 'Base de données',
  updatedAtPrefix: 'Mis à jour',
  updatedTimeUnavailable: 'Heure de mise à jour indisponible',
  originalSourceUnknown: "Source d'origine non suivie.",
  analysisEyebrow: 'Analyse',
  analysisTitle: 'Conversation analytique',
  analysisCopy:
      "Posez des questions de suivi naturellement. L'assistant utilise les vues déjà chargées dans l'entrepôt pour expliquer les données avec des graphiques et une synthèse concise.",
  noAnalysisYet: "Pas encore d'analyse.",
  noAnalysisCopy:
      "Chargez des tables sources à gauche, puis commencez vos questions ici. Cet espace est conçu pour une exploration itérative, pas seulement pour des requêtes ponctuelles.",
  aiAnalysis: 'Analyse IA',
  sourcesPrefix: 'Sources',
  qualityFlagsPrefix: 'Indicateurs de qualité',
  composerHelper:
      'Continuez avec des questions de suivi ici. La conversation reste dans le contexte de cette session.',
  showDebugData: 'Afficher les données de debug',
  chatPlaceholder:
      'Posez des questions sur les revenus, les rendez-vous, la facturation, les analyses ou approfondissez une réponse précédente.',
  analyzing: 'Analyse...',
  send: 'Envoyer',
  modalEnterCredentials: 'Entrer les identifiants source',
  modalEnterCredentialsCopy:
      'Cette actualisation a besoin des identifiants source avant de continuer.',
  refreshOneFrom: 'Actualiser {table} depuis {host}:{port} / {database}',
  refreshAllFrom: 'Actualiser toutes les tables suivies depuis {host}:{port}',
  cancel: 'Annuler',
  retryRefresh: "Relancer l'actualisation",
  modalLoadDataFirst: "Charger des données d'abord",
  modalLoadDataFirstCopy:
      "Connectez une source et ingérez au moins une table avant de démarrer l'analyse.",
  modalLoadDataFirstHelper:
      "Utilisez le connecteur de base de données à gauche pour choisir une source, charger des tables et les intégrer dans l'entrepôt.",
  close: 'Fermer',
  askFailed: "La requête d'analyse a échoué.",
  askFailedAnswer: "Je n'ai pas pu terminer cette demande d'analyse.",
  connectionFailed: 'La connexion a échoué.',
  chooseDatabaseFirst: "Choisissez d'abord une base de données.",
  couldNotLoadTables: 'Impossible de charger les tables.',
  selectTableBeforeLoad: 'Sélectionnez au moins une table avant le chargement.',
  ingestionFailed: "L'ingestion a échoué.",
  refreshFailed: "L'actualisation a échoué.",
  couldNotDeleteLoadedTable: 'Impossible de supprimer la table chargée.',
  couldNotDeleteLoadedGroup:
      'Impossible de supprimer ce groupe de sources chargées.',
  loadedSummary:
      "{count} table{suffix} chargée{suffix2} dans l'entrepôt et couche d'analyse actualisée.",
  processedSummary:
      '{count} table{suffix} sélectionnée{suffix2} traitée{suffix2}.',
  selectedTablesSummary:
      '{count} table{suffix} sélectionnée{suffix2} pour ingestion.',
  loadedDot: 'chargée',
  confirmDeleteGroup: 'Supprimer toutes les tables chargées pour {label} ?',
  resultSeries: 'Résultat',
  demoQuestions: <String>[
    'Quel mois génère le plus de revenus ?',
    'Montre la tendance hebdomadaire des revenus.',
    'Quels médecins ont le plus de rendez-vous complétés ?',
    'Combien de factures sont payées, impayées ou rejetées ?',
    "Quels sont les examens sanguins les plus fréquents ?",
  ],
);
