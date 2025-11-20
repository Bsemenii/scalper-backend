// src/components/TradesPanel.jsx
import React from "react";
import TradesTable from "./TradesTable";

/**
 * Простий адаптер, щоб старий код, який імпортує TradesPanel,
 * працював із новим компонентом TradesTable.
 */
const TradesPanel = (props) => {
  return <TradesTable {...props} />;
};

export default TradesPanel;
